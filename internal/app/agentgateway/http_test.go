package agentgateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

type deliveryHandlerTestStore struct {
	*callbackTestStore
	receipt domain.AgentGatewayDeliveryReceipt
}

func (s *deliveryHandlerTestStore) CreateDelivery(context.Context, string, domain.AgentGatewayDeliveryRequest, []byte) (domain.AgentGatewayDeliveryReceipt, bool, error) {
	return s.receipt, true, nil
}

func TestPostDeliveryReturnsPersistedReceiptStateOnIdempotentReplay(t *testing.T) {
	for _, test := range []struct {
		name          string
		receipt       domain.AgentGatewayDeliveryReceipt
		wantStatus    string
		wantMessageID int
		wantErrorCode string
	}{
		{name: "delivered replay", receipt: domain.AgentGatewayDeliveryReceipt{ReceiptID: 42, Status: "delivered", ResultTelegramMessageID: 91}, wantStatus: "sent", wantMessageID: 91},
		{name: "in progress replay", receipt: domain.AgentGatewayDeliveryReceipt{ReceiptID: 43, Status: "dispatching"}, wantStatus: "pending"},
		{name: "failed replay", receipt: domain.AgentGatewayDeliveryReceipt{ReceiptID: 44, Status: "failed"}, wantStatus: "failed", wantErrorCode: "telegram_delivery_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, err := json.Marshal(domain.AgentGatewayDeliveryRequest{
				IdempotencyKey: "delivery-1", GatewayInboundID: "inbound-1", TelegramOwnerID: 77,
				ReplyToUpdateID: "41", Purpose: "setup", Operation: "send", Text: "hello",
			})
			if err != nil {
				t.Fatal(err)
			}
			key := []byte(callbackTestSecret)
			nonce := base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef"))
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			req := httptest.NewRequest(http.MethodPost, deliveriesPath, bytes.NewReader(body))
			req.Header.Set(headerAgentGatewayID, "gateway-1")
			req.Header.Set(headerKeyID, "key-1")
			req.Header.Set(headerTimestamp, timestamp)
			req.Header.Set(headerNonce, nonce)
			req.Header.Set(headerSignature, makeSignature(key, req.Method, req.URL.EscapedPath(), timestamp, nonce, body))
			store := &deliveryHandlerTestStore{
				callbackTestStore: &callbackTestStore{}, receipt: test.receipt,
			}
			service := &Service{
				store: store, keyring: map[string]map[string][]byte{"gateway-1": {"key-1": key}},
				logger: zap.NewNop(), wake: make(chan struct{}, 1),
			}
			response := httptest.NewRecorder()
			(&deliveryHandler{service: service}).ServeHTTP(response, req)
			if response.Code != http.StatusAccepted {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
			var accepted deliveryAccepted
			if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
				t.Fatal(err)
			}
			if accepted.ReceiptID != strconv.FormatInt(test.receipt.ReceiptID, 10) || accepted.Status != test.wantStatus ||
				accepted.TelegramMessageID != test.wantMessageID || accepted.ErrorCode != test.wantErrorCode {
				t.Fatalf("response = %+v, want status=%q message=%d error=%q", accepted, test.wantStatus, test.wantMessageID, test.wantErrorCode)
			}
		})
	}
}
