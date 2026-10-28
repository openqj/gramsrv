package agentgateway

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

const (
	botAPIChannelChatIDBase int64 = 1000000000000
	messageInboundPath            = "/internal/agent/v1/inbound/messages"
	groupMessageInboundPath       = "/internal/agent/v1/inbound/group-messages"
	callbackInboundPath           = "/internal/agent/v1/inbound/callbacks"
	groupAuthorizationPath        = "/internal/agent/v1/group/authorize"
	maxBodyBytes                  = 64 << 10
	defaultLease                  = 15 * time.Second
)

type Store interface {
	EnqueueCallback(context.Context, domain.BotCallbackQuery) (domain.AgentGatewayInbound, error)
	ClaimInbound(context.Context, string, int, time.Duration) ([]domain.AgentGatewayInbound, error)
	ClaimCallback(context.Context, int64, string, time.Duration) (domain.AgentGatewayInbound, bool, error)
	AcceptInbound(context.Context, string, domain.AgentGatewayInbound, domain.AgentGatewayInboundAck) error
	FailInbound(context.Context, string, domain.AgentGatewayInbound, string, time.Time, bool) error
	Inbound(context.Context, int64) (domain.AgentGatewayInbound, bool, error)
	ConsumeNonce(context.Context, string, string, string, time.Time) (bool, error)
	CleanupNonces(context.Context, time.Time, int) error
	CreateDelivery(context.Context, string, domain.AgentGatewayDeliveryRequest, []byte) (domain.AgentGatewayDeliveryReceipt, bool, error)
	ClaimDeliveries(context.Context, string, int, time.Duration) ([]domain.AgentGatewayDeliveryReceipt, error)
	CompleteDelivery(context.Context, string, domain.AgentGatewayDeliveryReceipt, int) error
	FailDelivery(context.Context, string, domain.AgentGatewayDeliveryReceipt, string, time.Time, bool) error
	DeliveryReceipt(context.Context, string, int64) (domain.AgentGatewayDeliveryReceipt, bool, error)
}

type TelegramDelivery interface {
	BotAPISendMessageWithRandomID(context.Context, int64, int64, int64, string, []domain.MessageEntity, *domain.MessageReplyMarkup, bool, bool, int) (domain.Message, error)
	BotAPIEditMessageText(context.Context, int64, int64, int, string, []domain.MessageEntity, bool, *domain.MessageReplyMarkup, bool) (domain.Message, error)
}

type GroupAuthority interface {
	GetParticipant(context.Context, int64, int64, int64) (domain.ChannelMember, error)
	GetChannel(context.Context, int64, int64) (domain.ChannelView, error)
}

type AgentBotProfileResolver interface {
	BotInfo(context.Context, int64) (domain.BotProfile, bool, error)
}

type agentBotOptInChecker interface {
	AgentBotOptedIn(context.Context, int64, int64) (bool, error)
}

type acceptedInboundSourceStore interface {
	InboundByGatewayID(context.Context, string) (domain.AgentGatewayInbound, bool, error)
}

type PrivateEventConsumer interface {
	AcceptGatewayEvent(context.Context, domain.PrivateAgentGatewayEvent) error
}

type Config struct {
	GatewayBaseURL string
	InstanceID     string
	GatewayID      string
	OutboundKeyID  string
	OutboundSecret string
	GatewayKeys    map[string]map[string]string
	GroupAuthority GroupAuthority
	BotProfiles    AgentBotProfileResolver
	HTTPTimeout    time.Duration
	BatchSize      int
	PollInterval   time.Duration
}

func ParseGatewayKeysJSON(raw string) (map[string]map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var keys map[string]map[string]string
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return nil, fmt.Errorf("parse Gateway HMAC keyring")
	}
	return keys, nil
}

type Service struct {
	store          Store
	telegram       TelegramDelivery
	groupAuthority GroupAuthority
	botProfiles    AgentBotProfileResolver
	config         Config
	client         *http.Client
	baseURL        *url.URL
	workerID       string
	logger         *zap.Logger
	wake           chan struct{}
	keyring        map[string]map[string][]byte
	privateEvents  PrivateEventConsumer
}

func New(cfg Config, store Store, telegram TelegramDelivery, logger *zap.Logger) (*Service, error) {
	if strings.TrimSpace(cfg.GatewayBaseURL) == "" {
		return nil, nil
	}
	if store == nil || telegram == nil || cfg.GroupAuthority == nil || cfg.BotProfiles == nil || strings.TrimSpace(cfg.InstanceID) == "" ||
		strings.TrimSpace(cfg.OutboundKeyID) == "" || !validHMACSecret(cfg.OutboundSecret) || len(cfg.GatewayKeys) == 0 {
		return nil, fmt.Errorf("agent gateway requires store, Telegram delivery, instance identity, and both HMAC keyrings")
	}
	u, err := parseGatewayBaseURL(cfg.GatewayBaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 3 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 32
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	workerID, err := randomID()
	if err != nil {
		return nil, fmt.Errorf("create agent gateway worker identity: %w", err)
	}
	if cfg.GatewayID != "" {
		keyring := cfg.GatewayKeys[cfg.GatewayID]
		if len(keyring) == 0 {
			return nil, fmt.Errorf("agent gateway keyring does not contain the configured Gateway ID")
		}
		cfg.GatewayKeys = map[string]map[string]string{cfg.GatewayID: keyring}
	}
	keys := make(map[string]map[string][]byte, len(cfg.GatewayKeys))
	for gatewayID, keyring := range cfg.GatewayKeys {
		if strings.TrimSpace(gatewayID) == "" || len(keyring) == 0 {
			return nil, fmt.Errorf("agent gateway keyring has empty identity")
		}
		keys[gatewayID] = make(map[string][]byte, len(keyring))
		for keyID, secret := range keyring {
			if strings.TrimSpace(keyID) == "" || !validHMACSecret(secret) {
				return nil, fmt.Errorf("agent gateway keyring has empty key id or a key shorter than 32 bytes")
			}
			keys[gatewayID][keyID] = []byte(secret)
		}
	}
	client := &http.Client{Timeout: cfg.HTTPTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Service{store: store, telegram: telegram, groupAuthority: cfg.GroupAuthority, botProfiles: cfg.BotProfiles, config: cfg, client: client, baseURL: u,
		workerID: cfg.InstanceID + ":" + workerID, logger: logger, wake: make(chan struct{}, 1), keyring: keys}, nil
}

func parseGatewayBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" || u.RawPath != "" ||
		u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("agent gateway base URL must be an HTTP(S) origin without query, fragment, userinfo")
	}
	if u.Scheme == "http" {
		host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
		ip := net.ParseIP(host)
		isLoopbackIP := ip != nil && (ip.IsLoopback() || (ip.To4() != nil && ip.To4().IsLoopback()))
		if host != "localhost" && !isLoopbackIP {
			return nil, fmt.Errorf("agent gateway must use HTTPS unless its HTTP origin is loopback")
		}
	}
	return u, nil
}

func validHMACSecret(secret string) bool { return len([]byte(secret)) >= 32 }

func (s *Service) GatewayHTTPHandler() http.Handler {
	return &deliveryHandler{service: s}
}

func (s *Service) SetPrivateEventConsumer(consumer PrivateEventConsumer) {
	if s != nil {
		s.privateEvents = consumer
	}
}

func (s *Service) Run(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(s.config.PollInterval)
	cleanup := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	defer cleanup.Stop()
	s.dispatch(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		case <-cleanup.C:
			if err := s.store.CleanupNonces(ctx, time.Now().Add(-time.Minute), 5000); err != nil {
				s.logger.Warn("cleanup agent gateway HMAC nonces", zap.Error(err))
			}
		}
		s.dispatch(ctx)
	}
}

func (s *Service) dispatch(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	items, err := s.store.ClaimInbound(ctx, s.workerID, s.config.BatchSize, defaultLease)
	if err != nil {
		s.logger.Warn("claim agent gateway inbound events", zap.Error(err))
	} else {
		for _, item := range items {
			s.deliverInbound(ctx, item)
		}
	}
	deliveries, err := s.store.ClaimDeliveries(ctx, s.workerID, s.config.BatchSize, defaultLease)
	if err != nil {
		s.logger.Warn("claim agent gateway Telegram deliveries", zap.Error(err))
		return
	}
	for _, delivery := range deliveries {
		s.deliverTelegram(ctx, delivery)
	}
}

func (s *Service) deliverInbound(ctx context.Context, item domain.AgentGatewayInbound) {
	path := messageInboundPath
	if item.Kind == "callback" {
		path = callbackInboundPath
	} else if item.Kind == "group_message" {
		path = groupMessageInboundPath
	}
	endpoint := *s.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(string(item.Payload)))
	if err != nil {
		s.failInbound(item, err.Error(), true)
		return
	}
	if err := signRequest(req, []byte(s.config.OutboundSecret), s.config.InstanceID, s.config.OutboundKeyID, item.Payload); err != nil {
		s.failInbound(item, err.Error(), true)
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.failInbound(item, err.Error(), true)
		return
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if readErr != nil || len(body) > maxBodyBytes {
		s.failInbound(item, "invalid Gateway response body", true)
		return
	}
	if resp.StatusCode != http.StatusAccepted {
		retry := resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests ||
			(resp.StatusCode >= 500 && resp.StatusCode < 600)
		s.failInbound(item, fmt.Sprintf("Gateway returned HTTP %d", resp.StatusCode), retry)
		return
	}
	var ack domain.AgentGatewayInboundAck
	if err := decodeStrict(body, &ack); err != nil {
		s.failInbound(item, "invalid Gateway acceptance response", true)
		return
	}
	if err := s.store.AcceptInbound(ctx, s.workerID, item, ack); err != nil {
		s.logger.Warn("persist agent gateway acceptance", zap.Int64("update_id", item.UpdateID), zap.Error(err))
	}
}

func (s *Service) failInbound(item domain.AgentGatewayInbound, message string, retry bool) {
	delay := retryDelay(item.Attempts)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.store.FailInbound(ctx, s.workerID, item, message, time.Now().Add(delay), retry); err != nil {
		s.logger.Warn("record agent gateway ingress failure", zap.Int64("update_id", item.UpdateID), zap.Error(err))
	}
}

func (s *Service) deliverTelegram(ctx context.Context, receipt domain.AgentGatewayDeliveryReceipt) {
	var req domain.AgentGatewayDeliveryRequest
	if err := decodeStrict(receipt.RequestBody, &req); err != nil {
		s.failDelivery(receipt, err.Error(), false)
		return
	}
	markup, setMarkup, err := telegramMarkup(req.InlineKeyboard)
	if err != nil {
		s.failDelivery(receipt, err.Error(), false)
		return
	}
	sourceStore, ok := s.store.(acceptedInboundSourceStore)
	if !ok {
		s.failDelivery(receipt, "accepted Gateway inbound source lookup is unavailable", false)
		return
	}
	source, found, err := sourceStore.InboundByGatewayID(ctx, receipt.GatewayInboundID)
	if err != nil || !found {
		s.failDelivery(receipt, "accepted Gateway inbound source is unavailable", false)
		return
	}
	profile, profileFound, profileErr := s.botProfiles.BotInfo(ctx, source.ChatBotID)
	if profileErr != nil || !profileFound || profile.BotUserID != source.ChatBotID || profile.OwnerUserID != source.TelegramOwnerUserID {
		s.failDelivery(receipt, "Agent Bot ownership is no longer valid", profileErr != nil)
		return
	}
	chatID, replyToMessageID := source.TelegramOwnerUserID, 0
	if req.Purpose == "group_task_update" {
		if source.Kind != "group_message" || source.TelegramChatID <= 0 {
			s.failDelivery(receipt, "group delivery source is invalid", false)
			return
		}
		actor, actorErr := s.groupAuthority.GetParticipant(ctx, source.TelegramActorUserID, source.TelegramChatID, source.TelegramActorUserID)
		if actorErr != nil || actor.Status != domain.ChannelMemberActive {
			s.failDelivery(receipt, "group task actor is no longer an active member", actorErr != nil && !noGroupMembership(actorErr))
			return
		}
		botView, botErr := s.groupAuthority.GetChannel(ctx, source.ChatBotID, source.TelegramChatID)
		if botErr != nil || !botCanSendGroupMessage(botView) {
			s.failDelivery(receipt, "Agent Bot can no longer send messages to the source group", botErr != nil && !noGroupMembership(botErr))
			return
		}
		chatID = -botAPIChannelChatIDBase - source.TelegramChatID
		replyToMessageID = source.TopMessageID
		if replyToMessageID == 0 {
			replyToMessageID = source.TelegramMessageID
		}
	} else if req.Purpose == "owner_approval" {
		if source.Kind != "group_message" || source.TelegramOwnerUserID <= 0 {
			s.failDelivery(receipt, "owner approval source is invalid", false)
			return
		}
		chatID = source.TelegramOwnerUserID
	}
	var result domain.Message
	if req.Operation == "send" {
		result, err = s.telegram.BotAPISendMessageWithRandomID(ctx, source.ChatBotID,
			chatID, deliveryRandomID(receipt.GatewayID, receipt.IdempotencyKey),
			req.Text, nil, markup, false, false, replyToMessageID)
	} else {
		result, err = s.telegram.BotAPIEditMessageText(ctx, source.ChatBotID, chatID,
			req.TelegramMessageID, req.Text, nil, setMarkup, markup, false)
		if err != nil && strings.Contains(strings.ToUpper(err.Error()), "MESSAGE_NOT_MODIFIED") {
			err = nil
			result.ID = req.TelegramMessageID
		}
	}
	if err != nil {
		s.failDelivery(receipt, err.Error(), retryTelegramDelivery(err))
		return
	}
	if result.ID <= 0 {
		s.failDelivery(receipt, "Telegram delivery returned no message id", true)
		return
	}
	if err := s.store.CompleteDelivery(ctx, s.workerID, receipt, result.ID); err != nil {
		s.logger.Warn("complete agent gateway Telegram delivery", zap.Int64("receipt_id", receipt.ReceiptID), zap.Error(err))
	}
}

func (s *Service) failDelivery(receipt domain.AgentGatewayDeliveryReceipt, message string, retry bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.store.FailDelivery(ctx, s.workerID, receipt, message, time.Now().Add(retryDelay(receipt.Attempts)), retry); err != nil {
		s.logger.Warn("record agent gateway Telegram delivery failure", zap.Int64("receipt_id", receipt.ReceiptID), zap.Error(err))
	}
}

func (s *Service) OnAgentBotCallback(ctx context.Context, query domain.BotCallbackQuery) (domain.BotCallbackAnswer, bool, error) {
	if s == nil || query.BotUserID <= 0 {
		return domain.BotCallbackAnswer{}, false, nil
	}
	item, err := s.store.EnqueueCallback(ctx, query)
	if errors.Is(err, domain.ErrAgentGatewayBinding) {
		return domain.BotCallbackAnswer{}, false, nil
	}
	if err != nil {
		return domain.BotCallbackAnswer{}, true, err
	}
	if item.Status == "accepted" {
		return callbackAnswer(item), true, nil
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.HTTPTimeout)
	defer cancel()
	claimed, ok, err := s.store.ClaimCallback(ctx, item.UpdateID, s.workerID, defaultLease)
	if err != nil {
		return domain.BotCallbackAnswer{}, true, err
	}
	if ok {
		s.deliverCallbackNow(ctx, claimed)
	}
	for {
		latest, found, err := s.store.Inbound(ctx, item.UpdateID)
		if err != nil {
			return domain.BotCallbackAnswer{}, true, err
		}
		if found && latest.Status == "accepted" {
			return callbackAnswer(latest), true, nil
		}
		if !found || latest.Status == "failed" || ctx.Err() != nil {
			return domain.BotCallbackAnswer{}, true, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *Service) OnChatBotCallback(ctx context.Context, query domain.BotCallbackQuery) (domain.BotCallbackAnswer, bool, error) {
	return s.OnAgentBotCallback(ctx, query)
}

func (s *Service) deliverCallbackNow(ctx context.Context, item domain.AgentGatewayInbound) {
	path := callbackInboundPath
	endpoint := *s.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawPath = ""
	for attempt := 0; attempt < maxCallbackHTTPAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(string(item.Payload)))
		if err == nil {
			err = signRequest(req, []byte(s.config.OutboundSecret), s.config.InstanceID, s.config.OutboundKeyID, item.Payload)
		}
		if err != nil {
			s.failCallback(ctx, item, err.Error(), false)
			return
		}
		resp, err := s.client.Do(req)
		if err != nil {
			if ctx.Err() == nil && waitCallbackRetry(ctx, attempt) {
				continue
			}
			s.failCallback(ctx, item, err.Error(), true)
			return
		}
		if resp.StatusCode != http.StatusAccepted {
			status := resp.StatusCode
			_ = resp.Body.Close()
			retry := retryCallbackHTTPStatus(status)
			if retry && ctx.Err() == nil && waitCallbackRetry(ctx, attempt) {
				continue
			}
			s.failCallback(ctx, item, fmt.Sprintf("Gateway returned HTTP %d", status), retry)
			return
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		_ = resp.Body.Close()
		if readErr != nil {
			if ctx.Err() == nil && waitCallbackRetry(ctx, attempt) {
				continue
			}
			s.failCallback(ctx, item, readErr.Error(), true)
			return
		}
		if len(body) > maxBodyBytes {
			s.failCallback(ctx, item, "invalid Gateway callback response", false)
			return
		}
		var ack domain.AgentGatewayInboundAck
		if err := decodeStrict(body, &ack); err != nil {
			s.failCallback(ctx, item, "invalid Gateway callback acceptance", false)
			return
		}
		if err := s.store.AcceptInbound(ctx, s.workerID, item, ack); err != nil {
			s.failCallback(ctx, item, err.Error(), false)
		}
		return
	}
}

const (
	maxCallbackHTTPAttempts = 3
	callbackRetryDelay      = 50 * time.Millisecond
)

func retryCallbackHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 && status < 600
}

func waitCallbackRetry(ctx context.Context, attempt int) bool {
	if attempt+1 >= maxCallbackHTTPAttempts {
		return false
	}
	timer := time.NewTimer(time.Duration(attempt+1) * callbackRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func (s *Service) failCallback(_ context.Context, item domain.AgentGatewayInbound, message string, retry bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.store.FailInbound(ctx, s.workerID, item, message, time.Now().Add(retryDelay(item.Attempts)), retry); err != nil {
		s.logger.Warn("record agent gateway callback failure", zap.Int64("update_id", item.UpdateID), zap.Error(err))
	}
}

func callbackAnswer(item domain.AgentGatewayInbound) domain.BotCallbackAnswer {
	if item.CallbackAnswer == nil {
		return domain.BotCallbackAnswer{}
	}
	return item.CallbackAnswer.BotCallbackAnswer()
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	seconds := math.Pow(2, float64(min(attempt-1, 8)))
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds * float64(time.Second))
}

func retryTelegramDelivery(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToUpper(err.Error())
	for _, code := range []string{
		"BOT_INVALID", "CHAT_ID_INVALID", "USER_ID_INVALID", "MESSAGE_EMPTY", "MESSAGE_TOO_LONG",
		"MESSAGE_ID_INVALID", "CHAT_WRITE_FORBIDDEN", "REPLY_MARKUP_INVALID", "BUTTON_DATA_INVALID",
		"BUTTON_URL_INVALID", "BUTTON_TYPE_INVALID", "BUTTON_INVALID",
	} {
		if strings.Contains(message, code) {
			return false
		}
	}
	return true
}

func randomID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", raw), nil
}
