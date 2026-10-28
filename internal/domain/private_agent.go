package domain

import (
	"encoding/json"
	"errors"
)

var ErrPrivateAgentSessionNotFound = errors.New("private Agent session not found")

const (
	PrivateAgentSessionTitleEventType = "session.title_updated"
	MaxPrivateAgentSessionTitleRunes  = 64
)

// PrivateAgentSession maps an owner-authenticated Gateway session to a private
// MTProto compatibility peer. GatewaySessionID is authoritative; PeerID is a
// transport handle that is meaningful only with OwnerUserID.
type PrivateAgentSession struct {
	OwnerUserID          int64
	PeerID               int64
	GatewaySessionID     string
	CreateIdempotencyKey string
	AfterVersion         int64
	Title                string
}

// PrivateAgentGatewayEvent is the append-only event shape delivered by the
// independent Gateway over the Gramsrv HMAC boundary.
type PrivateAgentGatewayEvent struct {
	IdempotencyKey  string          `json:"idempotency_key"`
	TelegramOwnerID int64           `json:"telegram_owner_user_id"`
	SessionID       string          `json:"session_id"`
	TaskID          string          `json:"task_id"`
	EventID         string          `json:"event_id"`
	EventType       string          `json:"type"`
	LegacyEventType string          `json:"event_type,omitempty"`
	Status          string          `json:"status"`
	ResultText      string          `json:"result_text"`
	Version         int64           `json:"version"`
	Payload         json.RawMessage `json:"payload"`
	CreatedAt       string          `json:"created_at"`
}

type PrivateAgentSnapshotEvent struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"type"`
	LegacyType string          `json:"event_type,omitempty"`
	Status     string          `json:"status"`
	ResultText string          `json:"result_text"`
	Version    int64           `json:"version"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  string          `json:"created_at"`
	TaskID     string          `json:"task_id,omitempty"`
}

type PrivateAgentSnapshotTurn struct {
	TaskID string                      `json:"task_id"`
	Events []PrivateAgentSnapshotEvent `json:"events"`
}

type PrivateAgentSnapshot struct {
	SessionID        string                      `json:"session_id"`
	Turns            []PrivateAgentSnapshotTurn  `json:"turns"`
	Events           []PrivateAgentSnapshotEvent `json:"events,omitempty"`
	NextAfterVersion int64                       `json:"next_after_version"`
	HasMore          bool                        `json:"has_more"`
}
