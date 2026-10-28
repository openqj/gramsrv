package agentgateway

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"telesrv/internal/domain"
)

const (
	headerGramsrvInstanceID = "X-Gramsrv-Instance-ID"
	headerAgentGatewayID    = "X-Agent-Gateway-ID"
	headerKeyID             = "X-Key-ID"
	headerTimestamp         = "X-Timestamp"
	headerNonce             = "X-Nonce"
	headerSignature         = "X-Signature"
)

func signRequest(req *http.Request, secret []byte, identity, keyID string, body []byte) error {
	var nonceRaw [16]byte
	if _, err := rand.Read(nonceRaw[:]); err != nil {
		return err
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := base64.RawURLEncoding.EncodeToString(nonceRaw[:])
	path := req.URL.EscapedPath()
	signature := makeSignature(secret, req.Method, path, timestamp, nonce, body)
	if identity == "" || keyID == "" || signature == "" {
		return fmt.Errorf("invalid agent gateway signing configuration")
	}
	if identityHeader := identityHeaderFor(req.URL.Path); identityHeader != "" {
		req.Header.Set(identityHeader, identity)
	} else {
		return fmt.Errorf("invalid agent gateway request path")
	}
	req.Header.Set(headerKeyID, keyID)
	req.Header.Set(headerTimestamp, timestamp)
	req.Header.Set(headerNonce, nonce)
	req.Header.Set(headerSignature, signature)
	req.Header.Set("Content-Type", "application/json")
	return nil
}

// SignPrivateGatewayRequest signs owner-scoped private Agent session requests
// with the same Gramsrv-to-Gateway HMAC boundary used by the delivery adapter.
func SignPrivateGatewayRequest(req *http.Request, secret []byte, identity, keyID string, body []byte) error {
	if req == nil || !strings.HasPrefix(req.URL.Path, "/internal/agent/v1/private/") {
		return fmt.Errorf("invalid private Agent Gateway request path")
	}
	return signRequest(req, secret, identity, keyID, body)
}

func identityHeaderFor(path string) string {
	if strings.HasPrefix(path, "/internal/agent/v1/inbound/") || strings.HasPrefix(path, "/internal/agent/v1/private/") {
		return headerGramsrvInstanceID
	}
	return ""
}

func makeSignature(secret []byte, method, escapedPath, timestamp, nonce string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	canonical := method + "\n" + escapedPath + "\n" + timestamp + "\n" + nonce + "\n" + hex.EncodeToString(bodyHash[:])
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func verifyRequest(r *http.Request, body []byte, keyring map[string]map[string][]byte, now time.Time) (gatewayID, keyID, nonce string, err error) {
	if r.URL.RawQuery != "" {
		return "", "", "", fmt.Errorf("query strings are not allowed")
	}
	if r.URL.Path != deliveriesPath && !strings.HasPrefix(r.URL.Path, deliveriesPath+"/") && r.URL.Path != groupAuthorizationPath && r.URL.Path != privateEventsPath {
		return "", "", "", fmt.Errorf("unsupported agent gateway path")
	}
	gatewayID = strings.TrimSpace(r.Header.Get(headerAgentGatewayID))
	keyID = strings.TrimSpace(r.Header.Get(headerKeyID))
	timestampRaw := r.Header.Get(headerTimestamp)
	nonce = r.Header.Get(headerNonce)
	signatureRaw := r.Header.Get(headerSignature)
	key := keyring[gatewayID][keyID]
	if gatewayID == "" || keyID == "" || len(key) < 32 {
		return "", "", "", fmt.Errorf("unknown Gateway identity or key")
	}
	timestamp, parseErr := strconv.ParseInt(timestampRaw, 10, 64)
	if parseErr != nil || timestamp < now.Unix()-30 || timestamp > now.Unix()+30 {
		return "", "", "", fmt.Errorf("stale or invalid timestamp")
	}
	nonceBytes, decodeErr := base64.RawURLEncoding.Strict().DecodeString(nonce)
	if decodeErr != nil || len(nonceBytes) != 16 {
		return "", "", "", fmt.Errorf("invalid nonce")
	}
	signature, decodeErr := hex.DecodeString(signatureRaw)
	if decodeErr != nil || len(signature) != sha256.Size {
		return "", "", "", fmt.Errorf("invalid signature")
	}
	expected, _ := hex.DecodeString(makeSignature(key, r.Method, r.URL.EscapedPath(), timestampRaw, nonce, body))
	if !hmac.Equal(signature, expected) {
		return "", "", "", fmt.Errorf("signature mismatch")
	}
	return gatewayID, keyID, nonce, nil
}

func readBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBodyBytes {
		return nil, fmt.Errorf("body too large")
	}
	return body, nil
}

func decodeStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

func telegramMarkup(keyboard *[][]domain.AgentGatewayInlineButton) (*domain.MessageReplyMarkup, bool, error) {
	if keyboard == nil {
		return nil, false, nil
	}
	markup := &domain.MessageReplyMarkup{Type: domain.MessageReplyMarkupInline}
	for _, row := range *keyboard {
		buttons := make([]domain.MarkupButton, 0, len(row))
		for _, button := range row {
			if strings.TrimSpace(button.Text) == "" || !utf8.ValidString(button.Text) ||
				button.CallbackData == "" || !utf8.ValidString(button.CallbackData) || len([]byte(button.CallbackData)) > 64 {
				return nil, false, fmt.Errorf("invalid Gateway inline callback button")
			}
			buttons = append(buttons, domain.MarkupButton{Type: domain.MarkupButtonCallback, Text: button.Text, Data: []byte(button.CallbackData)})
		}
		markup.Inline = append(markup.Inline, buttons)
	}
	if len(markup.Inline) == 0 {
		return nil, true, nil
	}
	if err := domain.ValidateReplyMarkup(markup); err != nil {
		return nil, false, err
	}
	return markup, true, nil
}

func deliveryRandomID(gatewayID, idempotencyKey string) int64 {
	digest := sha256.Sum256([]byte(gatewayID + "\n" + idempotencyKey))
	id := int64(binary.BigEndian.Uint64(digest[:8]))
	if id == 0 {
		return 1
	}
	return id
}

func callbackBody(raw []byte) bool {
	return len(raw) <= maxBodyBytes
}
