package agentgateway

import (
	"encoding/base64"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"telesrv/internal/domain"
)

func TestGatewayRequestSignature(t *testing.T) {
	secret := []byte("test-key-012345678901234567890123")
	body := []byte(`{"ok":true}`)
	nonce := base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req := httptest.NewRequest("POST", deliveriesPath, nil)
	req.Header.Set(headerAgentGatewayID, "gateway-a")
	req.Header.Set(headerKeyID, "key-1")
	req.Header.Set(headerTimestamp, timestamp)
	req.Header.Set(headerNonce, nonce)
	req.Header.Set(headerSignature, makeSignature(secret, req.Method, req.URL.EscapedPath(), timestamp, nonce, body))

	gatewayID, keyID, gotNonce, err := verifyRequest(req, body, map[string]map[string][]byte{"gateway-a": {"key-1": secret}}, time.Now())
	if err != nil || gatewayID != "gateway-a" || keyID != "key-1" || gotNonce != nonce {
		t.Fatalf("verifyRequest = (%q, %q, %q, %v)", gatewayID, keyID, gotNonce, err)
	}
	if _, _, _, err := verifyRequest(req, append(body, ' '), map[string]map[string][]byte{"gateway-a": {"key-1": secret}}, time.Now()); err == nil {
		t.Fatal("signature accepted a changed body")
	}
	req.URL.RawQuery = "unexpected=1"
	if _, _, _, err := verifyRequest(req, body, map[string]map[string][]byte{"gateway-a": {"key-1": secret}}, time.Now()); err == nil {
		t.Fatal("signature accepted a query string")
	}
}

func TestHMACKeyMinimumLength(t *testing.T) {
	if validHMACSecret("0123456789abcdef0123456789abcdef") == false {
		t.Fatal("32-byte HMAC key was rejected")
	}
	if validHMACSecret("0123456789abcdef0123456789abcde") {
		t.Fatal("31-byte HMAC key was accepted")
	}
}

func TestDeliveryValidationAndCallbackKeyboard(t *testing.T) {
	valid := domain.AgentGatewayDeliveryRequest{
		IdempotencyKey: "task:1", GatewayInboundID: "inbound-1", GatewayTaskID: "task-1",
		TelegramOwnerID: 101, ReplyToUpdateID: "12", Purpose: "task_update", Operation: "send", Text: "Working",
	}
	if !validDeliveryRequest(valid) {
		t.Fatal("valid task update rejected")
	}
	valid.Operation = "edit"
	valid.TelegramMessageID = 5
	if !validDeliveryRequest(valid) {
		t.Fatal("valid task edit rejected")
	}
	valid.Purpose = "setup"
	valid.GatewayTaskID = ""
	valid.TelegramMessageID = 0
	valid.Operation = "send"
	if !validDeliveryRequest(valid) {
		t.Fatal("valid setup reply rejected")
	}
	valid.GatewayProfileID = "unexpected"
	if validDeliveryRequest(valid) {
		t.Fatal("setup reply accepted an unexpected profile binding")
	}

	var keyboard [][]domain.AgentGatewayInlineButton
	if err := decodeStrict([]byte(`[[{"text":"Approve","callback_data":"opaque"}]]`), &keyboard); err != nil {
		t.Fatalf("decode callback keyboard: %v", err)
	}
	markup, set, err := telegramMarkup(&keyboard)
	if err != nil || !set || markup == nil || len(markup.Inline) != 1 || len(markup.Inline[0]) != 1 {
		t.Fatalf("telegramMarkup = (%+v, %v, %v)", markup, set, err)
	}
	if err := decodeStrict([]byte(`[[{"text":"Open","callback_data":"opaque","url":"https://example.com"}]]`), &keyboard); err == nil {
		t.Fatal("URL button field was accepted")
	}
	keyboard = [][]domain.AgentGatewayInlineButton{{{Text: "Approve", CallbackData: string(make([]byte, 65))}}}
	if _, _, err := telegramMarkup(&keyboard); err == nil {
		t.Fatal("callback data longer than 64 UTF-8 bytes was accepted")
	}
}
