package privateagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"telesrv/internal/app/agentgateway"
	"telesrv/internal/domain"
)

const (
	maxGatewayBodyBytes = 1 << 20
	defaultTimeout      = 5 * time.Second
	maxSnapshotPages    = 10
)

var ErrSessionNotFound = domain.ErrPrivateAgentSessionNotFound

type Store interface {
	CreatePrivateAgentSession(context.Context, int64, string, string) (domain.PrivateAgentSession, error)
	GetPrivateAgentSessionByPeer(context.Context, int64, int64) (domain.PrivateAgentSession, bool, error)
	GetPrivateAgentSessionByMessageID(context.Context, int64, int) (domain.PrivateAgentSession, bool, error)
	GetPrivateAgentSessionByGatewayID(context.Context, int64, string) (domain.PrivateAgentSession, bool, error)
	ListPrivateAgentSessions(context.Context, int64, int64, int) ([]domain.PrivateAgentSession, int, error)
	RecordPrivateAgentTask(context.Context, int64, string, string, string) error
	PrivateAgentTaskMessageID(context.Context, int64, string, string) (int, bool, error)
	SetPrivateAgentCursor(context.Context, int64, string, int64) error
}

type titleStore interface {
	SetPrivateAgentSessionTitle(context.Context, int64, string, string, string) (domain.PrivateAgentSession, bool, error)
	MarkPrivateAgentSessionTitleUpdateDelivered(context.Context, int64, string, string) error
}

type TitleUpdatePublisher interface {
	PushSessionTitle(context.Context, int64, int64, string) error
}

type MessageService interface {
	SendPrivateText(context.Context, int64, domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error)
}

type Config struct {
	GatewayBaseURL string
	InstanceID     string
	KeyID          string
	Secret         string
	HTTPTimeout    time.Duration
	TitleUpdates   TitleUpdatePublisher
}

type Service struct {
	store        Store
	messages     MessageService
	config       Config
	client       *http.Client
	baseURL      *url.URL
	logger       *zap.Logger
	titleUpdates TitleUpdatePublisher
}

func New(cfg Config, sessions Store, messages MessageService, logger *zap.Logger) (*Service, error) {
	if strings.TrimSpace(cfg.GatewayBaseURL) == "" {
		return nil, nil
	}
	if sessions == nil || messages == nil || strings.TrimSpace(cfg.InstanceID) == "" || strings.TrimSpace(cfg.KeyID) == "" || len([]byte(cfg.Secret)) < 32 {
		return nil, fmt.Errorf("private Agent Gateway requires session/message stores and a valid HMAC identity")
	}
	baseURL, err := parseBaseURL(cfg.GatewayBaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = defaultTimeout
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{
		store: sessions, messages: messages, config: cfg, baseURL: baseURL,
		client: &http.Client{Timeout: cfg.HTTPTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		logger: logger, titleUpdates: cfg.TitleUpdates,
	}, nil
}

func parseBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("private Agent Gateway URL must be an HTTP(S) origin")
	}
	if u.Scheme == "http" {
		host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, fmt.Errorf("private Agent Gateway must use HTTPS outside loopback")
		}
	}
	return u, nil
}

type createSessionRequest struct {
	OwnerUserID    int64  `json:"telegram_owner_user_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

type createSessionResponse struct {
	SessionID string `json:"session_id"`
}

type sendTurnRequest struct {
	OwnerUserID    int64  `json:"telegram_owner_user_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Text           string `json:"text"`
}

type sendTurnResponse struct {
	SessionID string `json:"session_id"`
	TaskID    string `json:"task_id"`
	Status    string `json:"status"`
}

type snapshotRequest struct {
	OwnerUserID  int64 `json:"telegram_owner_user_id"`
	AfterVersion int64 `json:"after_version"`
	Limit        int   `json:"limit"`
}

func (s *Service) CreateConversation(ctx context.Context, ownerUserID int64, idempotencyKey string) (domain.PrivateAgentSession, error) {
	if s == nil || ownerUserID <= 0 || idempotencyKey == "" {
		return domain.PrivateAgentSession{}, fmt.Errorf("private Agent create requires authenticated owner and idempotency key")
	}
	var response createSessionResponse
	path := "/internal/agent/v1/private/sessions"
	if err := s.postJSON(ctx, path, createSessionRequest{OwnerUserID: ownerUserID, IdempotencyKey: idempotencyKey}, &response); err != nil {
		return domain.PrivateAgentSession{}, err
	}
	if !validOpaqueID(response.SessionID) {
		return domain.PrivateAgentSession{}, fmt.Errorf("Gateway returned an invalid private Agent session ID")
	}
	return s.store.CreatePrivateAgentSession(ctx, ownerUserID, response.SessionID, idempotencyKey)
}

func (s *Service) ListConversations(ctx context.Context, ownerUserID, offsetPeerID int64, limit int) ([]domain.PrivateAgentSession, int, error) {
	if s == nil || ownerUserID <= 0 {
		return nil, 0, fmt.Errorf("private Agent list requires authenticated owner")
	}
	return s.store.ListPrivateAgentSessions(ctx, ownerUserID, offsetPeerID, clampLimit(limit))
}

func (s *Service) ResolveConversation(ctx context.Context, ownerUserID, peerID int64) (domain.PrivateAgentSession, bool, error) {
	if s == nil || ownerUserID <= 0 || !domain.IsAgentCompatibilityPeerID(peerID) {
		return domain.PrivateAgentSession{}, false, nil
	}
	return s.store.GetPrivateAgentSessionByPeer(ctx, ownerUserID, peerID)
}

func (s *Service) ResolveConversationByMessageID(ctx context.Context, ownerUserID int64, messageID int) (domain.PrivateAgentSession, bool, error) {
	if s == nil || ownerUserID <= 0 || messageID <= 0 {
		return domain.PrivateAgentSession{}, false, nil
	}
	return s.store.GetPrivateAgentSessionByMessageID(ctx, ownerUserID, messageID)
}

func (s *Service) SendMessage(ctx context.Context, ownerUserID, peerID int64, request domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, string, error) {
	session, found, err := s.ResolveConversation(ctx, ownerUserID, peerID)
	if err != nil {
		return domain.SendPrivateTextResult{}, "", err
	}
	if !found {
		return domain.SendPrivateTextResult{}, "", ErrSessionNotFound
	}
	if request.RandomID == 0 || request.Message == "" || !utf8.ValidString(request.Message) {
		return domain.SendPrivateTextResult{}, "", domain.ErrMessageEmpty
	}
	idempotencyKey := fmt.Sprintf("telegram:%d:%s:%d", ownerUserID, session.GatewaySessionID, request.RandomID)
	var response sendTurnResponse
	path := "/internal/agent/v1/private/sessions/" + url.PathEscape(session.GatewaySessionID) + "/turns"
	if err := s.postJSON(ctx, path, sendTurnRequest{OwnerUserID: ownerUserID, IdempotencyKey: idempotencyKey, Text: request.Message}, &response); err != nil {
		return domain.SendPrivateTextResult{}, "", err
	}
	if response.SessionID != session.GatewaySessionID || !validOpaqueID(response.TaskID) {
		return domain.SendPrivateTextResult{}, "", fmt.Errorf("Gateway returned an invalid private Agent turn")
	}
	if err := s.store.RecordPrivateAgentTask(ctx, ownerUserID, session.GatewaySessionID, response.TaskID, idempotencyKey); err != nil {
		return domain.SendPrivateTextResult{}, "", err
	}
	request.SenderUserID = ownerUserID
	request.RecipientUserID = ownerUserID
	request.AgentConversationPeerID = session.PeerID
	request.AgentSessionID = session.GatewaySessionID
	request.AgentTaskID = response.TaskID
	request.AgentEventID = "input:" + response.TaskID
	request.AgentRole = "user"
	result, err := s.messages.SendPrivateText(ctx, ownerUserID, request)
	if err != nil {
		return domain.SendPrivateTextResult{}, "", err
	}
	return result, response.TaskID, nil
}

func (s *Service) AcceptGatewayEvent(ctx context.Context, event domain.PrivateAgentGatewayEvent) error {
	if event.EventType != "" && event.LegacyEventType != "" && event.EventType != event.LegacyEventType {
		return fmt.Errorf("private Agent event type fields disagree")
	}
	if event.EventType == "" {
		event.EventType = event.LegacyEventType
	}
	if s == nil || event.TelegramOwnerID <= 0 || !validOpaqueID(event.SessionID) || !validOpaqueID(event.TaskID) ||
		!validOpaqueID(event.EventID) || event.EventID != event.IdempotencyKey || event.Version <= 0 || event.EventType == "" ||
		!json.Valid(event.Payload) || len(event.Payload) > maxGatewayBodyBytes || !utf8.ValidString(event.ResultText) ||
		utf8.RuneCountInString(event.ResultText) > domain.MaxMessageTextLength {
		return fmt.Errorf("invalid private Agent Gateway event")
	}
	session, found, err := s.store.GetPrivateAgentSessionByGatewayID(ctx, event.TelegramOwnerID, event.SessionID)
	if err != nil {
		return err
	}
	if !found {
		return ErrSessionNotFound
	}
	if err := s.store.RecordPrivateAgentTask(ctx, event.TelegramOwnerID, event.SessionID, event.TaskID, ""); err != nil {
		return err
	}
	if event.EventType == domain.PrivateAgentSessionTitleEventType {
		return s.acceptSessionTitleUpdated(ctx, event)
	}
	messageID, found, err := s.store.PrivateAgentTaskMessageID(ctx, event.TelegramOwnerID, event.SessionID, event.TaskID)
	if err != nil {
		return err
	}
	var replyTo *domain.MessageReply
	if found && messageID > 0 {
		replyTo = &domain.MessageReply{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: session.PeerID}, MessageID: messageID}
	}
	date := time.Now().Unix()
	if parsed, parseErr := time.Parse(time.RFC3339Nano, event.CreatedAt); parseErr == nil {
		date = parsed.Unix()
	}
	text := gatewayEventText(event)
	if text == "" {
		return nil
	}
	randomID := eventRandomID(event.EventID)
	_, err = s.messages.SendPrivateText(ctx, event.TelegramOwnerID, domain.SendPrivateTextRequest{
		SenderUserID: event.TelegramOwnerID, RecipientUserID: event.TelegramOwnerID,
		RandomID: randomID, Message: text, Date: int(date), ReplyTo: replyTo,
		AgentConversationPeerID: session.PeerID, AgentSessionID: session.GatewaySessionID,
		AgentTaskID: event.TaskID, AgentEventID: event.EventID, AgentRole: "assistant",
		AgentEventType: event.EventType, AgentStatus: event.Status, AgentEventVersion: event.Version,
		AgentPayload: append([]byte(nil), event.Payload...), AgentIncoming: true,
	})
	return err
}

func (s *Service) acceptSessionTitleUpdated(ctx context.Context, event domain.PrivateAgentGatewayEvent) error {
	var payload struct {
		Title string `json:"title"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode private Agent session title: %w", err)
	}
	title := strings.TrimSpace(payload.Title)
	if title == "" || !utf8.ValidString(title) || utf8.RuneCountInString(title) > domain.MaxPrivateAgentSessionTitleRunes {
		return fmt.Errorf("invalid private Agent session title")
	}
	store, ok := s.store.(titleStore)
	if !ok {
		return fmt.Errorf("private Agent session title store unavailable")
	}
	session, needsPush, err := store.SetPrivateAgentSessionTitle(ctx, event.TelegramOwnerID, event.SessionID, event.EventID, title)
	if err != nil {
		return err
	}
	if !needsPush {
		return nil
	}
	if session.OwnerUserID != event.TelegramOwnerID || session.GatewaySessionID != event.SessionID ||
		!domain.IsAgentCompatibilityPeerID(session.PeerID) || session.Title != title {
		return fmt.Errorf("private Agent title mapping mismatch")
	}
	if s.titleUpdates == nil {
		return fmt.Errorf("private Agent session title updates unavailable")
	}
	if err := s.titleUpdates.PushSessionTitle(ctx, event.TelegramOwnerID, session.PeerID, title); err != nil {
		return fmt.Errorf("push private Agent session title: %w", err)
	}
	if err := store.MarkPrivateAgentSessionTitleUpdateDelivered(ctx, event.TelegramOwnerID, event.SessionID, event.EventID); err != nil {
		return fmt.Errorf("mark private Agent session title delivered: %w", err)
	}
	return nil
}

func (s *Service) SyncSnapshot(ctx context.Context, ownerUserID, peerID int64) error {
	session, found, err := s.ResolveConversation(ctx, ownerUserID, peerID)
	if err != nil {
		return err
	}
	if !found {
		return ErrSessionNotFound
	}
	for page := 0; page < maxSnapshotPages; page++ {
		session, found, err = s.store.GetPrivateAgentSessionByGatewayID(ctx, ownerUserID, session.GatewaySessionID)
		if err != nil || !found {
			return err
		}
		var snapshot domain.PrivateAgentSnapshot
		path := "/internal/agent/v1/private/sessions/" + url.PathEscape(session.GatewaySessionID) + "/snapshot"
		if err := s.postJSON(ctx, path, snapshotRequest{OwnerUserID: ownerUserID, AfterVersion: session.AfterVersion, Limit: 100}, &snapshot); err != nil {
			return err
		}
		if snapshot.SessionID != session.GatewaySessionID || snapshot.NextAfterVersion < session.AfterVersion {
			return fmt.Errorf("Gateway returned an invalid private Agent snapshot cursor")
		}
		for _, event := range flattenSnapshot(snapshot) {
			if err := s.AcceptGatewayEvent(ctx, domain.PrivateAgentGatewayEvent{
				IdempotencyKey: event.EventID, TelegramOwnerID: ownerUserID, SessionID: session.GatewaySessionID,
				TaskID: event.TaskID, EventID: event.EventID, EventType: event.EventType, LegacyEventType: event.LegacyType, Status: event.Status,
				ResultText: event.ResultText, Version: event.Version, Payload: event.Payload, CreatedAt: event.CreatedAt,
			}); err != nil {
				return err
			}
		}
		if err := s.store.SetPrivateAgentCursor(ctx, ownerUserID, session.GatewaySessionID, snapshot.NextAfterVersion); err != nil {
			return err
		}
		if !snapshot.HasMore || snapshot.NextAfterVersion == session.AfterVersion {
			return nil
		}
	}
	return nil
}

func flattenSnapshot(snapshot domain.PrivateAgentSnapshot) []domain.PrivateAgentSnapshotEvent {
	out := append([]domain.PrivateAgentSnapshotEvent(nil), snapshot.Events...)
	for _, turn := range snapshot.Turns {
		for _, event := range turn.Events {
			if event.TaskID == "" {
				event.TaskID = turn.TaskID
			}
			out = append(out, event)
		}
	}
	return out
}

func (s *Service) postJSON(ctx context.Context, path string, request any, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode private Agent Gateway request: %w", err)
	}
	endpoint := *s.baseURL
	endpoint.Path = path
	endpoint.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create private Agent Gateway request: %w", err)
	}
	if err := agentgateway.SignPrivateGatewayRequest(req, []byte(s.config.Secret), s.config.InstanceID, s.config.KeyID, body); err != nil {
		return fmt.Errorf("sign private Agent Gateway request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("call private Agent Gateway: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxGatewayBodyBytes+1))
	if err != nil || len(data) > maxGatewayBodyBytes {
		return fmt.Errorf("read private Agent Gateway response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("private Agent Gateway returned HTTP %d", resp.StatusCode)
	}
	if err := json.Unmarshal(data, response); err != nil {
		return fmt.Errorf("decode private Agent Gateway response: %w", err)
	}
	return nil
}

func validOpaqueID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == ':' || r == '.') {
			return false
		}
	}
	return true
}

func clampLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func eventRandomID(eventID string) int64 {
	digest := sha256.Sum256([]byte("private-agent-event:v1\n" + eventID))
	id := int64(binary.BigEndian.Uint64(digest[:8]) & (1<<63 - 1))
	if id == 0 {
		return 1
	}
	return id
}

func gatewayEventText(event domain.PrivateAgentGatewayEvent) string {
	if event.ResultText != "" {
		return event.ResultText
	}
	var payload map[string]any
	if json.Unmarshal(event.Payload, &payload) == nil {
		if activity, ok := payload["activity_text"].(string); ok && activity != "" && utf8.ValidString(activity) && utf8.RuneCountInString(activity) <= domain.MaxMessageTextLength {
			return "Agent · " + activity
		}
	}
	if event.Status != "" {
		return "Agent · " + strings.ToLower(event.Status)
	}
	return "Agent update"
}
