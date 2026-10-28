package agentgateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

type callbackTestStore struct {
	item           domain.AgentGatewayInbound
	failures       int
	lastFailRetry  bool
	failContextErr error
}

func (s *callbackTestStore) EnqueueCallback(context.Context, domain.BotCallbackQuery) (domain.AgentGatewayInbound, error) {
	return s.item, nil
}
func (*callbackTestStore) ClaimInbound(context.Context, string, int, time.Duration) ([]domain.AgentGatewayInbound, error) {
	return nil, nil
}
func (s *callbackTestStore) ClaimCallback(_ context.Context, _ int64, _ string, _ time.Duration) (domain.AgentGatewayInbound, bool, error) {
	s.item.Status = "dispatching"
	s.item.Attempts++
	return s.item, true, nil
}
func (s *callbackTestStore) AcceptInbound(_ context.Context, _ string, _ domain.AgentGatewayInbound, ack domain.AgentGatewayInboundAck) error {
	s.item.Status = "accepted"
	s.item.GatewayInboundID = ack.GatewayInboundID
	s.item.GatewayProfileID = ack.ProfileID
	s.item.GatewayTaskID = ack.TaskID
	s.item.Disposition = ack.Disposition
	s.item.CallbackAnswer = ack.Answer
	return nil
}
func (s *callbackTestStore) FailInbound(ctx context.Context, _ string, _ domain.AgentGatewayInbound, _ string, _ time.Time, retry bool) error {
	s.failures++
	s.lastFailRetry = retry
	s.failContextErr = ctx.Err()
	if retry {
		s.item.Status = "pending"
	} else {
		s.item.Status = "failed"
	}
	return nil
}
func (s *callbackTestStore) Inbound(context.Context, int64) (domain.AgentGatewayInbound, bool, error) {
	return s.item, true, nil
}
func (*callbackTestStore) ConsumeNonce(context.Context, string, string, string, time.Time) (bool, error) {
	return true, nil
}
func (*callbackTestStore) CleanupNonces(context.Context, time.Time, int) error { return nil }
func (*callbackTestStore) CreateDelivery(context.Context, string, domain.AgentGatewayDeliveryRequest, []byte) (domain.AgentGatewayDeliveryReceipt, bool, error) {
	return domain.AgentGatewayDeliveryReceipt{}, false, nil
}
func (*callbackTestStore) ClaimDeliveries(context.Context, string, int, time.Duration) ([]domain.AgentGatewayDeliveryReceipt, error) {
	return nil, nil
}
func (*callbackTestStore) CompleteDelivery(context.Context, string, domain.AgentGatewayDeliveryReceipt, int) error {
	return nil
}
func (*callbackTestStore) FailDelivery(context.Context, string, domain.AgentGatewayDeliveryReceipt, string, time.Time, bool) error {
	return nil
}
func (*callbackTestStore) DeliveryReceipt(context.Context, string, int64) (domain.AgentGatewayDeliveryReceipt, bool, error) {
	return domain.AgentGatewayDeliveryReceipt{}, false, nil
}

type callbackTestTelegram struct{}

const callbackTestSecret = "test-key-012345678901234567890123"

func (callbackTestTelegram) BotAPISendMessageWithRandomID(context.Context, int64, int64, int64, string, []domain.MessageEntity, *domain.MessageReplyMarkup, bool, bool, int) (domain.Message, error) {
	return domain.Message{}, nil
}
func (callbackTestTelegram) BotAPIEditMessageText(context.Context, int64, int64, int, string, []domain.MessageEntity, bool, *domain.MessageReplyMarkup, bool) (domain.Message, error) {
	return domain.Message{}, nil
}

func newCallbackTestService(t *testing.T, store *callbackTestStore, server *httptest.Server) *Service {
	t.Helper()
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &Service{
		store: store, telegram: callbackTestTelegram{},
		config: Config{InstanceID: "gramsrv-a", OutboundKeyID: "key-1", OutboundSecret: callbackTestSecret, HTTPTimeout: time.Second},
		client: server.Client(), baseURL: baseURL, workerID: "worker-1", logger: zap.NewNop(), wake: make(chan struct{}, 1),
	}
}

func TestCallbackIsForwardedSynchronouslyAndReturnsIgnoredAnswer(t *testing.T) {
	payload, err := json.Marshal(domain.AgentGatewayCallbackRequest{
		UpdateID: "41", CallbackQueryID: "41", TelegramOwnerUserID: 77,
		BotUserID: domain.ChatBotUserID, TelegramChatID: 77, TelegramMessageID: 9, CallbackData: "opaque-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &callbackTestStore{item: domain.AgentGatewayInbound{
		UpdateID: 41, Kind: "callback", Payload: payload, Status: "pending", Attempts: 0,
	}}
	secret := callbackTestSecret
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != callbackInboundPath {
			t.Errorf("Gateway request = %s %s", r.Method, r.URL.Path)
		}
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Errorf("read callback body: %v", readErr)
		}
		instanceID := r.Header.Get(headerGramsrvInstanceID)
		keyID := r.Header.Get(headerKeyID)
		timestamp := r.Header.Get(headerTimestamp)
		nonce := r.Header.Get(headerNonce)
		signature := r.Header.Get(headerSignature)
		if instanceID != "gramsrv-a" || keyID != "key-1" || signature != makeSignature([]byte(secret), r.Method, r.URL.EscapedPath(), timestamp, nonce, body) {
			t.Errorf("invalid signed callback request identity=%q key=%q", instanceID, keyID)
		}
		var got domain.AgentGatewayCallbackRequest
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("decode callback body: %v", err)
		}
		if got.UpdateID != "41" || got.CallbackQueryID != "41" || got.TelegramOwnerUserID != 77 || got.BotUserID != domain.ChatBotUserID {
			t.Errorf("callback body = %+v", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"accepted":true,"update_id":"41","gateway_inbound_id":"gw-inbound-1","disposition":"callback_ignored","duplicate":false,"answer":{"text":"This button expired.","show_alert":true}}`)
	}))
	defer server.Close()
	service := newCallbackTestService(t, store, server)
	answer, handled, err := service.OnChatBotCallback(context.Background(), domain.BotCallbackQuery{
		BotUserID: domain.ChatBotUserID, UserID: 77,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 77}, MessageID: 9, Data: []byte("opaque-token"),
	})
	if err != nil || !handled || answer.Message != "This button expired." || !answer.Alert {
		t.Fatalf("callback answer = %+v, handled=%v, err=%v", answer, handled, err)
	}
}

func TestParseGatewayBaseURLRequiresHTTPSOutsideLoopback(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "loopback ipv4 http", url: "http://127.0.0.1:18083"},
		{name: "loopback ipv6 http", url: "http://[::1]:18083"},
		{name: "localhost http", url: "http://localhost:18083"},
		{name: "remote https", url: "https://gateway.internal:18083"},
		{name: "remote http", url: "http://gateway.internal:18083", wantErr: true},
		{name: "public ipv4 http", url: "http://192.0.2.4:18083", wantErr: true},
		{name: "query", url: "https://gateway.internal?token=x", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseGatewayBaseURL(test.url)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseGatewayBaseURL(%q) error = %v, wantErr %v", test.url, err, test.wantErr)
			}
		})
	}
}

func TestCallbackFailurePersistsAfterRequestContextExpires(t *testing.T) {
	store := &callbackTestStore{item: domain.AgentGatewayInbound{UpdateID: 41, Attempts: 2}}
	service := &Service{store: store, workerID: "worker-1", logger: zap.NewNop()}
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	service.failCallback(requestCtx, store.item, "Gateway unavailable", true)
	if store.failures != 1 || !store.lastFailRetry || store.failContextErr != nil || store.item.Status != "pending" {
		t.Fatalf("failure persistence = count %d retry %v contextErr %v status %q", store.failures, store.lastFailRetry, store.failContextErr, store.item.Status)
	}
}

func TestCallbackRetriesTransientHTTPAndDoesNotRetryClientErrors(t *testing.T) {
	for _, tc := range []struct {
		name          string
		statuses      []int
		wantCalls     int
		wantStatus    string
		wantAnswer    string
		wantFailures  int
		wantFailRetry bool
	}{
		{name: "503 then 202", statuses: []int{http.StatusServiceUnavailable, http.StatusAccepted}, wantCalls: 2, wantStatus: "accepted", wantAnswer: "This button expired."},
		{name: "503 retries exhausted remain pending", statuses: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable}, wantCalls: 3, wantStatus: "pending", wantFailures: 1, wantFailRetry: true},
		{name: "401 is terminal", statuses: []int{http.StatusUnauthorized}, wantCalls: 1, wantStatus: "failed", wantFailures: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(domain.AgentGatewayCallbackRequest{
				UpdateID: "41", CallbackQueryID: "41", TelegramOwnerUserID: 77,
				BotUserID: domain.ChatBotUserID, TelegramChatID: 77, TelegramMessageID: 9, CallbackData: "opaque-token",
			})
			if err != nil {
				t.Fatal(err)
			}
			store := &callbackTestStore{item: domain.AgentGatewayInbound{
				UpdateID: 41, Kind: "callback", Payload: payload, Status: "pending",
			}}
			calls := 0
			var requestBodies []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				body, readErr := io.ReadAll(r.Body)
				if readErr != nil {
					t.Errorf("read callback body: %v", readErr)
				}
				requestBodies = append(requestBodies, string(body))
				var got domain.AgentGatewayCallbackRequest
				if err := json.Unmarshal(body, &got); err != nil {
					t.Errorf("decode callback body: %v", err)
				} else if got.UpdateID != "41" || got.CallbackQueryID != "41" {
					t.Errorf("callback IDs = update %q/query %q, want both 41", got.UpdateID, got.CallbackQueryID)
				}
				if calls > len(tc.statuses) {
					t.Errorf("unexpected callback retry %d", calls)
					http.Error(w, "unexpected retry", http.StatusInternalServerError)
					return
				}
				status := tc.statuses[calls-1]
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				if status == http.StatusAccepted {
					_, _ = io.WriteString(w, `{"accepted":true,"update_id":"41","gateway_inbound_id":"gw-inbound-1","disposition":"callback_ignored","duplicate":false,"answer":{"text":"This button expired.","show_alert":true}}`)
				}
			}))
			t.Cleanup(server.Close)

			service := newCallbackTestService(t, store, server)
			answer, handled, err := service.OnAgentBotCallback(context.Background(), domain.BotCallbackQuery{
				BotUserID: domain.ChatBotUserID, UserID: 77,
				Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 77}, MessageID: 9, Data: []byte("opaque-token"),
			})
			if err != nil || !handled {
				t.Fatalf("callback handled=%v, err=%v", handled, err)
			}
			if calls != tc.wantCalls {
				t.Fatalf("Gateway request count = %d, want %d", calls, tc.wantCalls)
			}
			if len(requestBodies) > 1 {
				for _, body := range requestBodies[1:] {
					if body != requestBodies[0] {
						t.Fatalf("retry callback payload changed: first=%s retry=%s", requestBodies[0], body)
					}
				}
			}
			if store.item.Status != tc.wantStatus {
				t.Fatalf("callback status = %q, want %q", store.item.Status, tc.wantStatus)
			}
			if answer.Message != tc.wantAnswer {
				t.Fatalf("callback answer = %+v, want message %q", answer, tc.wantAnswer)
			}
			if store.failures != tc.wantFailures || store.lastFailRetry != tc.wantFailRetry {
				t.Fatalf("failure writes = %d retry=%v, want %d retry=%v", store.failures, store.lastFailRetry, tc.wantFailures, tc.wantFailRetry)
			}
			if store.failContextErr != nil {
				t.Fatalf("failure persistence context error = %v", store.failContextErr)
			}
		})
	}
}
