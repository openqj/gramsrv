package agentgateway

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

type deliveryHandler struct{ service *Service }

type deliveryAccepted struct {
	ReceiptID         string `json:"receipt_id"`
	Status            string `json:"status"`
	TelegramMessageID int    `json:"telegram_message_id,omitempty"`
	ErrorCode         string `json:"error_code,omitempty"`
}

type deliveryReceiptResponse struct {
	ReceiptID         string `json:"receipt_id"`
	Status            string `json:"status"`
	TelegramMessageID int    `json:"telegram_message_id,omitempty"`
	ErrorCode         string `json:"error_code,omitempty"`
}

func (h *deliveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.service == nil {
		http.NotFound(w, r)
		return
	}
	if r.URL.RawQuery != "" {
		http.Error(w, "query strings are not allowed", http.StatusBadRequest)
		return
	}
	body, err := readBody(r)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusRequestEntityTooLarge)
		return
	}
	gatewayID, keyID, nonce, err := verifyRequest(r, body, h.service.keyring, time.Now())
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	consumed, err := h.service.store.ConsumeNonce(r.Context(), gatewayID, keyID, nonce, time.Now().Add(2*time.Minute))
	if err != nil {
		h.service.logger.Warn("persist Gateway request nonce", zap.Error(err))
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if !consumed {
		http.Error(w, "replayed request", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == groupAuthorizationPath {
		h.authorizeGroup(w, r, body)
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == privateEventsPath {
		h.postPrivateEvent(w, r, body)
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == deliveriesPath {
		h.postDelivery(w, r, gatewayID, body)
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, deliveriesPath+"/") {
		if len(body) != 0 {
			http.Error(w, "GET body is not allowed", http.StatusBadRequest)
			return
		}
		h.getDelivery(w, r, gatewayID)
		return
	}
	http.NotFound(w, r)
}

const deliveriesPath = "/internal/agent/v1/deliveries"
const privateEventsPath = "/internal/agent/v1/private/events"

func (h *deliveryHandler) postPrivateEvent(w http.ResponseWriter, r *http.Request, body []byte) {
	consumer := h.service.privateEvents
	if consumer == nil {
		http.Error(w, "private Agent delivery unavailable", http.StatusServiceUnavailable)
		return
	}
	var event domain.PrivateAgentGatewayEvent
	if err := decodeStrict(body, &event); err != nil {
		http.Error(w, "invalid private Agent event", http.StatusBadRequest)
		return
	}
	if err := consumer.AcceptGatewayEvent(r.Context(), event); err != nil {
		if errors.Is(err, domain.ErrPrivateAgentSessionNotFound) {
			http.Error(w, "private Agent session not found", http.StatusForbidden)
			return
		}
		h.service.logger.Warn("persist private Agent Gateway event", zap.Error(err))
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"accepted":true}`))
}

func (h *deliveryHandler) postDelivery(w http.ResponseWriter, r *http.Request, gatewayID string, raw []byte) {
	var request domain.AgentGatewayDeliveryRequest
	if err := decodeStrict(raw, &request); err != nil || !validDeliveryRequest(request) {
		http.Error(w, "invalid delivery request", http.StatusBadRequest)
		return
	}
	if _, _, err := telegramMarkup(request.InlineKeyboard); err != nil {
		http.Error(w, "invalid inline keyboard", http.StatusBadRequest)
		return
	}
	digest := sha256.Sum256(raw)
	receipt, _, err := h.service.store.CreateDelivery(r.Context(), gatewayID, request, digest[:])
	if errors.Is(err, domain.ErrAgentGatewayConflict) {
		http.Error(w, "idempotency key reused with a different request", http.StatusConflict)
		return
	}
	if errors.Is(err, domain.ErrAgentGatewayBinding) {
		http.Error(w, "inbound binding rejected", http.StatusForbidden)
		return
	}
	if err != nil {
		h.service.logger.Warn("enqueue Gateway delivery", zap.Error(err))
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	select {
	case h.service.wake <- struct{}{}:
	default:
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(deliveryAccepted{
		ReceiptID: strconv.FormatInt(receipt.ReceiptID, 10), Status: deliveryStatus(receipt.Status),
		TelegramMessageID: deliveredMessageID(receipt), ErrorCode: deliveryErrorCode(receipt),
	})
}

func (h *deliveryHandler) getDelivery(w http.ResponseWriter, r *http.Request, gatewayID string) {
	path := strings.TrimPrefix(r.URL.Path, deliveriesPath+"/")
	if path == "" || strings.Contains(path, "/") {
		http.NotFound(w, r)
		return
	}
	receiptID, err := strconv.ParseInt(path, 10, 64)
	if err != nil || receiptID <= 0 {
		http.Error(w, "invalid receipt id", http.StatusBadRequest)
		return
	}
	receipt, found, err := h.service.store.DeliveryReceipt(r.Context(), gatewayID, receiptID)
	if err != nil {
		h.service.logger.Warn("read Gateway delivery receipt", zap.Error(err))
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	response := deliveryReceiptResponse{
		ReceiptID: strconv.FormatInt(receipt.ReceiptID, 10), Status: deliveryStatus(receipt.Status),
		TelegramMessageID: deliveredMessageID(receipt),
	}
	if receipt.Status == "failed" {
		response.ErrorCode = deliveryErrorCode(receipt)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func deliveryStatus(status string) string {
	switch status {
	case "delivered":
		return "sent"
	case "dispatching":
		return "pending"
	default:
		return status
	}
}

func deliveredMessageID(receipt domain.AgentGatewayDeliveryReceipt) int {
	if receipt.Status == "delivered" {
		return receipt.ResultTelegramMessageID
	}
	return 0
}

func deliveryErrorCode(receipt domain.AgentGatewayDeliveryReceipt) string {
	if receipt.Status == "failed" {
		return "telegram_delivery_failed"
	}
	return ""
}

func validDeliveryRequest(req domain.AgentGatewayDeliveryRequest) bool {
	replyToUpdateID, parseErr := strconv.ParseInt(req.ReplyToUpdateID, 10, 64)
	if len(req.IdempotencyKey) == 0 || len(req.IdempotencyKey) > 160 || !utf8.ValidString(req.IdempotencyKey) || strings.TrimSpace(req.IdempotencyKey) == "" ||
		strings.TrimSpace(req.GatewayInboundID) == "" || len(req.GatewayInboundID) > 256 ||
		req.TelegramOwnerID <= 0 || parseErr != nil || replyToUpdateID <= 0 || strconv.FormatInt(replyToUpdateID, 10) != req.ReplyToUpdateID ||
		req.Text == "" || !utf8.ValidString(req.Text) || utf8.RuneCountInString(req.Text) > domain.MaxMessageTextLength {
		return false
	}
	if req.Operation != "send" && req.Operation != "edit" {
		return false
	}
	if req.GatewayTaskID != "" && (len(req.GatewayTaskID) > 256 || strings.TrimSpace(req.GatewayTaskID) == "") ||
		req.GatewayProfileID != "" && (len(req.GatewayProfileID) > 256 || strings.TrimSpace(req.GatewayProfileID) == "") {
		return false
	}
	if req.TelegramMessageID < 0 || req.TelegramMessageID > domain.MaxMessageBoxID {
		return false
	}
	if req.Operation == "send" && req.TelegramMessageID != 0 || req.Operation == "edit" && req.TelegramMessageID <= 0 {
		return false
	}
	switch req.Purpose {
	case "setup":
		return req.Operation == "send" && req.GatewayProfileID == "" && req.GatewayTaskID == ""
	case "profile_welcome":
		return req.Operation == "send" && req.GatewayProfileID != "" && req.GatewayTaskID == ""
	case "task_update":
		return req.GatewayProfileID == "" && req.GatewayTaskID != ""
	case "group_task_update":
		return (req.Operation == "send" || req.Operation == "edit") && req.InlineKeyboard == nil
	case "owner_approval":
		return req.Operation == "send" && req.GatewayProfileID != "" && req.GatewayTaskID != ""
	default:
		return false
	}
}
