package domain

import (
	"errors"
	"time"
)

var (
	ErrAgentGatewayConflict = errors.New("agent gateway idempotency conflict")
	ErrAgentGatewayBinding  = errors.New("agent gateway inbound binding rejected")
)

// AgentGatewayInbound is an authenticated Telegram message/callback snapshot
// queued for delivery to the separately owned Agent Gateway.
type AgentGatewayInbound struct {
	UpdateID            int64
	BotAPIUpdateID      int64
	Kind                string
	CallbackQueryID     int64
	TelegramOwnerUserID int64
	TelegramActorUserID int64
	ChatBotID           int64
	BotUsername         string
	TelegramChatID      int64
	TelegramMessageID   int
	ReplyToMessageID    int
	TopMessageID        int
	Text                string
	CallbackData        []byte
	Payload             []byte
	Attempts            int
	Status              string
	Disposition         string
	GatewayInboundID    string
	GatewayProfileID    string
	GatewayTaskID       string
	CallbackAnswer      *AgentGatewayCallbackAnswer
	LastError           string
}

// AgentGatewayGroupMessageRequest is derived from a persisted channel message
// and a server-resolved Agent Bot profile.
type AgentGatewayGroupMessageRequest struct {
	SchemaVersion    int                         `json:"schema_version"`
	UpdateID         string                      `json:"update_id"`
	BotAPIUpdateID   int64                       `json:"-"`
	BotUserID        int64                       `json:"bot_user_id"`
	BotUsername      string                      `json:"bot_username"`
	BotOwnerUserID   int64                       `json:"-"`
	ActorUserID      int64                       `json:"actor_user_id"`
	ChatType         string                      `json:"chat_type"`
	ChatID           int64                       `json:"chat_id"`
	MessageID        int                         `json:"message_id"`
	ReplyToMessageID int                         `json:"reply_to_message_id,omitempty"`
	TopMessageID     int                         `json:"top_message_id,omitempty"`
	Date             int                         `json:"date"`
	Text             string                      `json:"text"`
	Entities         []AgentGatewayMessageEntity `json:"entities,omitempty"`
	BotMentioned     bool                        `json:"bot_mentioned"`
	BotAddressed     bool                        `json:"bot_addressed"`
	DirectedCommand  string                      `json:"directed_command,omitempty"`
}

type AgentGatewayMessageEntity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
	UserID int64  `json:"user_id,omitempty"`
}

type AgentGatewayGroupAuthorizationRequest struct {
	SchemaVersion   int    `json:"schema_version"`
	ChatType        string `json:"chat_type"`
	ChatID          int64  `json:"chat_id"`
	BotUserID       int64  `json:"bot_user_id"`
	ActorUserID     int64  `json:"actor_user_id"`
	OwnerUserID     int64  `json:"owner_user_id"`
	Action          string `json:"action"`
	TargetUserID    int64  `json:"target_user_id,omitempty"`
	TargetBotUserID int64  `json:"target_bot_user_id,omitempty"`
}

type AgentGatewayGroupAuthorizationResponse struct {
	SchemaVersion           int    `json:"schema_version"`
	Allowed                 bool   `json:"allowed"`
	Reason                  string `json:"reason"`
	CheckedAt               int64  `json:"checked_at"`
	ActorMemberStatus       string `json:"actor_member_status"`
	OwnerMemberStatus       string `json:"owner_member_status"`
	TargetMemberStatus      string `json:"target_member_status"`
	BotMemberStatus         string `json:"bot_member_status"`
	BotCanSendMessage       bool   `json:"bot_can_send_message"`
	TargetBotMemberStatus   string `json:"target_bot_member_status"`
	TargetBotCanSendMessage bool   `json:"target_bot_can_send_message"`
	TargetBotRole           string `json:"target_bot_role"`
	ActorRole               string `json:"actor_role"`
	OwnerRole               string `json:"owner_role"`
	TargetRole              string `json:"target_role"`
	Action                  string `json:"action"`
}

type AgentGatewayMessageRequest struct {
	UpdateID            string `json:"update_id"`
	TelegramOwnerUserID int64  `json:"telegram_owner_user_id"`
	BotUserID           int64  `json:"bot_user_id"`
	BotOwnerUserID      int64  `json:"bot_owner_user_id"`
	BotUsername         string `json:"bot_username"`
	TelegramChatID      int64  `json:"telegram_chat_id"`
	TelegramMessageID   int    `json:"telegram_message_id"`
	Text                string `json:"text"`
}

type AgentGatewayCallbackRequest struct {
	UpdateID            string `json:"update_id"`
	CallbackQueryID     string `json:"callback_query_id"`
	TelegramOwnerUserID int64  `json:"telegram_owner_user_id"`
	BotUserID           int64  `json:"bot_user_id"`
	BotOwnerUserID      int64  `json:"bot_owner_user_id"`
	TelegramChatID      int64  `json:"telegram_chat_id"`
	TelegramMessageID   int    `json:"telegram_message_id"`
	CallbackData        string `json:"callback_data"`
}

// AgentGatewayInboundAck is the Gateway's durable acceptance response. The
// referenced IDs are handles only; Gramsrv does not own their task state.
type AgentGatewayInboundAck struct {
	Accepted         bool                        `json:"accepted"`
	UpdateID         string                      `json:"update_id"`
	GatewayInboundID string                      `json:"gateway_inbound_id"`
	ProfileID        string                      `json:"profile_id,omitempty"`
	TaskID           string                      `json:"task_id,omitempty"`
	Disposition      string                      `json:"disposition"`
	Duplicate        bool                        `json:"duplicate"`
	Answer           *AgentGatewayCallbackAnswer `json:"answer,omitempty"`
}

// AgentGatewayCallbackAnswer is the deliberately narrow callback response
// accepted by the Gramsrv adapter.
type AgentGatewayCallbackAnswer struct {
	Text      string `json:"text,omitempty"`
	ShowAlert bool   `json:"show_alert,omitempty"`
	CacheTime int    `json:"cache_time,omitempty"`
	URL       string `json:"url,omitempty"`
}

func (a AgentGatewayCallbackAnswer) BotCallbackAnswer() BotCallbackAnswer {
	return BotCallbackAnswer{Message: a.Text, Alert: a.ShowAlert, CacheTime: a.CacheTime, URL: a.URL}
}

type AgentGatewayInlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

type AgentGatewayDeliveryRequest struct {
	IdempotencyKey    string                        `json:"idempotency_key"`
	GatewayInboundID  string                        `json:"gateway_inbound_id"`
	GatewayTaskID     string                        `json:"gateway_task_id,omitempty"`
	GatewayProfileID  string                        `json:"gateway_profile_id,omitempty"`
	TelegramOwnerID   int64                         `json:"telegram_owner_user_id"`
	ReplyToUpdateID   string                        `json:"reply_to_update_id"`
	Purpose           string                        `json:"purpose"`
	Operation         string                        `json:"operation"`
	TelegramMessageID int                           `json:"telegram_message_id,omitempty"`
	Text              string                        `json:"text"`
	InlineKeyboard    *[][]AgentGatewayInlineButton `json:"inline_keyboard,omitempty"`
}

type AgentGatewayDeliveryReceipt struct {
	ReceiptID               int64
	GatewayID               string
	IdempotencyKey          string
	GatewayInboundID        string
	GatewayTaskID           string
	GatewayProfileID        string
	TelegramOwnerID         int64
	ReplyToUpdateID         int64
	Purpose                 string
	Operation               string
	TelegramMessageID       int
	Text                    string
	InlineKeyboardJSON      []byte
	RequestBody             []byte
	RequestHash             []byte
	Status                  string
	ResultTelegramMessageID int
	Attempts                int
	NextAttemptAt           time.Time
	LastError               string
}
