package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

var ErrAgentGatewayConflict = domain.ErrAgentGatewayConflict
var ErrAgentGatewayBinding = domain.ErrAgentGatewayBinding

type AgentGatewayStore struct{ db sqlcgen.DBTX }

func NewAgentGatewayStore(db sqlcgen.DBTX) *AgentGatewayStore {
	return &AgentGatewayStore{db: db}
}

func (s *AgentGatewayStore) EnqueueMessageTx(ctx context.Context, tx pgx.Tx, event domain.AgentGatewayInbound) error {
	if event.Kind != "message" || event.TelegramOwnerUserID <= 0 || event.TelegramActorUserID != event.TelegramOwnerUserID || event.ChatBotID <= 0 ||
		!validAgentGatewayBotUsername(event.BotUsername) ||
		event.TelegramChatID != event.TelegramOwnerUserID || event.TelegramMessageID <= 0 || event.Text == "" {
		return fmt.Errorf("invalid agent gateway message ingress")
	}
	var updateID int64
	if err := tx.QueryRow(ctx, `SELECT nextval('agent_gateway_update_id_seq')`).Scan(&updateID); err != nil {
		return fmt.Errorf("allocate agent gateway update id: %w", err)
	}
	body, err := json.Marshal(domain.AgentGatewayMessageRequest{
		UpdateID: strconv.FormatInt(updateID, 10), TelegramOwnerUserID: event.TelegramOwnerUserID,
		BotUserID: event.ChatBotID, BotOwnerUserID: event.TelegramOwnerUserID,
		BotUsername: event.BotUsername, TelegramChatID: event.TelegramChatID,
		TelegramMessageID: event.TelegramMessageID, Text: event.Text,
	})
	if err != nil {
		return fmt.Errorf("encode agent gateway message ingress: %w", err)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO agent_gateway_inbound
    (update_id, kind, owner_user_id, telegram_actor_user_id, chat_bot_id, telegram_chat_id,
     telegram_message_id, payload)
	VALUES ($1, 'message', $2, $3, $4, $5, $6, $7::jsonb)`,
		updateID, event.TelegramOwnerUserID, event.TelegramActorUserID, event.ChatBotID, event.TelegramChatID,
		event.TelegramMessageID, body)
	if err != nil {
		return fmt.Errorf("enqueue agent gateway message ingress: %w", err)
	}
	return nil
}

func (s *AgentGatewayStore) EnqueueGroupMessageTx(ctx context.Context, tx pgx.Tx, event domain.AgentGatewayGroupMessageRequest) error {
	if event.BotAPIUpdateID < 0 || event.SchemaVersion != 1 || event.BotUserID <= 0 || event.BotOwnerUserID <= 0 ||
		!validAgentGatewayBotUsername(event.BotUsername) || event.ActorUserID <= 0 ||
		event.ChatType != "channel" || event.ChatID <= 0 || event.MessageID <= 0 || event.Date <= 0 ||
		event.ReplyToMessageID < 0 || event.ReplyToMessageID > domain.MaxMessageBoxID ||
		event.TopMessageID < 0 || event.TopMessageID > domain.MaxMessageBoxID ||
		!utf8.ValidString(event.Text) || utf8.RuneCountInString(event.Text) > domain.MaxMessageTextLength ||
		(!event.BotMentioned && !event.BotAddressed && event.DirectedCommand == "") || !validAgentGatewayDirectedCommand(event.DirectedCommand) ||
		len(event.Entities) > domain.MaxMessageEntityCount {
		return ErrAgentGatewayBinding
	}
	optedIn, err := agentBotOptedIn(ctx, tx, event.BotUserID, event.BotOwnerUserID)
	if err != nil {
		return fmt.Errorf("check Agent Bot owner opt-in: %w", err)
	}
	if !optedIn {
		return nil
	}
	for _, entity := range event.Entities {
		if entity.Type == "" || entity.Offset < 0 || entity.Length <= 0 ||
			entity.Offset > 2*domain.MaxMessageTextLength || entity.Length > 2*domain.MaxMessageTextLength ||
			entity.Offset+entity.Length > 2*domain.MaxMessageTextLength || entity.UserID < 0 {
			return ErrAgentGatewayBinding
		}
	}
	var updateID int64
	if err := tx.QueryRow(ctx, `SELECT nextval('agent_gateway_update_id_seq')`).Scan(&updateID); err != nil {
		return fmt.Errorf("allocate agent gateway group update id: %w", err)
	}
	event.UpdateID = strconv.FormatInt(updateID, 10)
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode agent gateway group ingress: %w", err)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO agent_gateway_inbound
    (update_id, bot_api_update_id, kind, owner_user_id, telegram_actor_user_id, chat_bot_id, telegram_chat_id,
     telegram_message_id, telegram_reply_to_message_id, telegram_top_message_id, payload)
	VALUES ($1, NULLIF($2, 0), 'group_message', $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
ON CONFLICT DO NOTHING`,
		updateID, event.BotAPIUpdateID, event.BotOwnerUserID, event.ActorUserID, event.BotUserID, event.ChatID, event.MessageID,
		event.ReplyToMessageID, event.TopMessageID, body)
	if err != nil {
		return fmt.Errorf("enqueue agent gateway group ingress: %w", err)
	}
	return nil
}

func validAgentGatewayBotUsername(username string) bool {
	if len(username) < 5 || len(username) > 32 {
		return false
	}
	for _, r := range username {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

func validAgentGatewayDirectedCommand(command string) bool {
	switch command {
	case "", "agent collaboration on", "agent collaboration off":
		return true
	default:
		return false
	}
}

func agentBotOptedIn(ctx context.Context, db sqlcgen.DBTX, botUserID, ownerUserID int64) (bool, error) {
	var optedIn bool
	err := db.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM agent_gateway_inbound
    WHERE kind = 'message' AND chat_bot_id = $1
      AND owner_user_id = $2 AND status = 'accepted' AND disposition = 'opted_in'
      AND COALESCE(gateway_profile_id, '') <> ''
)`, botUserID, ownerUserID).Scan(&optedIn)
	return optedIn, err
}

func (s *AgentGatewayStore) AgentBotOptedIn(ctx context.Context, botUserID, ownerUserID int64) (bool, error) {
	if botUserID <= 0 || ownerUserID <= 0 {
		return false, nil
	}
	return agentBotOptedIn(ctx, s.db, botUserID, ownerUserID)
}

// AgentBotUsernames lists Bot identities with an accepted Agent profile for one owner.
func (s *AgentGatewayStore) AgentBotUsernames(ctx context.Context, ownerUserID int64) ([]string, error) {
	if s == nil || s.db == nil || ownerUserID <= 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
SELECT DISTINCT lower(u.username)
FROM agent_gateway_inbound i
JOIN users u ON u.id = i.chat_bot_id AND u.is_bot
WHERE i.kind = 'message' AND i.owner_user_id = $1
  AND i.status = 'accepted' AND COALESCE(i.gateway_profile_id, '') <> ''
  AND COALESCE(u.username, '') <> ''
ORDER BY lower(u.username)`, ownerUserID)
	if err != nil {
		return nil, fmt.Errorf("list account Agent bots: %w", err)
	}
	defer rows.Close()

	var usernames []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, fmt.Errorf("scan account Agent Bot username: %w", err)
		}
		usernames = append(usernames, username)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate account Agent Bot usernames: %w", err)
	}
	return usernames, nil
}

func (s *AgentGatewayStore) agentBotOwnerForCallback(ctx context.Context, botUserID, actorUserID int64) (int64, bool, error) {
	var ownerUserID int64
	err := s.db.QueryRow(ctx, `
SELECT owner_user_id
FROM agent_gateway_inbound
WHERE kind = 'message' AND chat_bot_id = $1 AND status = 'accepted'
  AND disposition = 'opted_in' AND COALESCE(gateway_profile_id, '') <> ''
ORDER BY update_id DESC LIMIT 1`, botUserID).Scan(&ownerUserID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return ownerUserID, ownerUserID == actorUserID, nil
}

func (s *AgentGatewayStore) EnqueueCallback(ctx context.Context, query domain.BotCallbackQuery) (domain.AgentGatewayInbound, error) {
	if query.BotUserID <= 0 || query.UserID <= 0 || query.MessageID <= 0 ||
		query.Peer != (domain.Peer{Type: domain.PeerTypeUser, ID: query.UserID}) || len(query.Data) == 0 || len(query.Data) > domain.MaxCallbackDataLen {
		return domain.AgentGatewayInbound{}, ErrAgentGatewayBinding
	}
	ownerUserID, optedIn, err := s.agentBotOwnerForCallback(ctx, query.BotUserID, query.UserID)
	if err != nil {
		return domain.AgentGatewayInbound{}, err
	}
	if !optedIn {
		return domain.AgentGatewayInbound{}, ErrAgentGatewayBinding
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.AgentGatewayInbound{}, fmt.Errorf("agent gateway callback: database does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.AgentGatewayInbound{}, fmt.Errorf("begin agent gateway callback: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var updateID int64
	if err := tx.QueryRow(ctx, `SELECT nextval('agent_gateway_update_id_seq')`).Scan(&updateID); err != nil {
		return domain.AgentGatewayInbound{}, fmt.Errorf("allocate agent gateway callback id: %w", err)
	}
	queryID := updateID
	body, err := json.Marshal(domain.AgentGatewayCallbackRequest{
		UpdateID: strconv.FormatInt(updateID, 10), CallbackQueryID: strconv.FormatInt(queryID, 10),
		TelegramOwnerUserID: query.UserID, BotUserID: query.BotUserID, BotOwnerUserID: ownerUserID,
		TelegramChatID: query.Peer.ID, TelegramMessageID: query.MessageID,
		CallbackData: string(query.Data),
	})
	if err != nil {
		return domain.AgentGatewayInbound{}, fmt.Errorf("encode agent gateway callback: %w", err)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO agent_gateway_inbound
    (update_id, kind, callback_query_id, owner_user_id, telegram_actor_user_id, chat_bot_id,
     telegram_chat_id, telegram_message_id, callback_data, payload)
VALUES ($1, 'callback', $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
ON CONFLICT (chat_bot_id, owner_user_id, telegram_chat_id, telegram_message_id, callback_data)
    WHERE kind = 'callback' DO NOTHING`,
		updateID, queryID, ownerUserID, query.UserID, query.BotUserID, query.Peer.ID, query.MessageID, query.Data, body)
	if err != nil {
		return domain.AgentGatewayInbound{}, fmt.Errorf("enqueue agent gateway callback: %w", err)
	}
	item, err := scanAgentGatewayInbound(tx.QueryRow(ctx, `
SELECT update_id, kind, callback_query_id, owner_user_id, telegram_actor_user_id, chat_bot_id,
       telegram_chat_id, telegram_message_id, callback_data, payload::text,
       attempts, status, disposition, COALESCE(gateway_inbound_id, ''),
       COALESCE(gateway_profile_id, ''), COALESCE(gateway_task_id, ''),
       callback_answer::text, last_error, COALESCE(bot_api_update_id, 0),
       COALESCE(telegram_reply_to_message_id, 0), COALESCE(telegram_top_message_id, 0)
FROM agent_gateway_inbound
WHERE owner_user_id = $1 AND telegram_chat_id = $2 AND telegram_message_id = $3
  AND chat_bot_id = $4 AND callback_data = $5 AND kind = 'callback'`, query.UserID, query.Peer.ID, query.MessageID, query.BotUserID, query.Data))
	if err != nil {
		return domain.AgentGatewayInbound{}, fmt.Errorf("read agent gateway callback: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.AgentGatewayInbound{}, fmt.Errorf("commit agent gateway callback: %w", err)
	}
	return item, nil
}

func (s *AgentGatewayStore) ClaimInbound(ctx context.Context, worker string, limit int, lease time.Duration) ([]domain.AgentGatewayInbound, error) {
	if worker == "" || limit <= 0 || lease <= 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
WITH claimable AS (
    SELECT update_id
    FROM agent_gateway_inbound
    WHERE kind IN ('message', 'group_message') AND ((status = 'pending' AND next_attempt_at <= now())
       OR (status = 'dispatching' AND lease_until <= now()))
    ORDER BY update_id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE agent_gateway_inbound AS i
SET status = 'dispatching', attempts = i.attempts + 1,
    lease_owner = $2, lease_until = now() + make_interval(secs => $3::double precision),
    updated_at = now()
FROM claimable
WHERE i.update_id = claimable.update_id
RETURNING i.update_id, i.kind, COALESCE(i.callback_query_id, 0), i.owner_user_id, i.telegram_actor_user_id,
          i.chat_bot_id, i.telegram_chat_id, i.telegram_message_id,
          COALESCE(i.callback_data, ''::bytea), i.payload::text, i.attempts,
          i.status, i.disposition, COALESCE(i.gateway_inbound_id, ''),
          COALESCE(i.gateway_profile_id, ''), COALESCE(i.gateway_task_id, ''),
          i.callback_answer::text, i.last_error, COALESCE(i.bot_api_update_id, 0),
          COALESCE(i.telegram_reply_to_message_id, 0), COALESCE(i.telegram_top_message_id, 0)
`, limit, worker, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim agent gateway ingress: %w", err)
	}
	defer rows.Close()
	var out []domain.AgentGatewayInbound
	for rows.Next() {
		item, err := scanAgentGatewayInbound(rows)
		if err != nil {
			return nil, fmt.Errorf("scan agent gateway ingress: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent gateway ingress: %w", err)
	}
	return out, nil
}

func (s *AgentGatewayStore) ClaimCallback(ctx context.Context, updateID int64, worker string, lease time.Duration) (domain.AgentGatewayInbound, bool, error) {
	if updateID <= 0 || worker == "" || lease <= 0 {
		return domain.AgentGatewayInbound{}, false, nil
	}
	item, err := scanAgentGatewayInbound(s.db.QueryRow(ctx, `
UPDATE agent_gateway_inbound
SET status = 'dispatching', attempts = attempts + 1, lease_owner = $2,
    lease_until = now() + make_interval(secs => $3::double precision), updated_at = now()
WHERE update_id = $1 AND kind = 'callback'
  AND (status IN ('pending', 'failed') OR (status = 'dispatching' AND lease_until <= now()))
RETURNING update_id, kind, callback_query_id, owner_user_id, telegram_actor_user_id, chat_bot_id,
          telegram_chat_id, telegram_message_id, callback_data, payload::text,
          attempts, status, disposition, COALESCE(gateway_inbound_id, ''),
          COALESCE(gateway_profile_id, ''), COALESCE(gateway_task_id, ''),
          callback_answer::text, last_error, COALESCE(bot_api_update_id, 0),
          COALESCE(telegram_reply_to_message_id, 0), COALESCE(telegram_top_message_id, 0)`, updateID, worker, lease.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentGatewayInbound{}, false, nil
	}
	return item, err == nil, err
}

type agentGatewayRowScanner interface{ Scan(...any) error }

func scanAgentGatewayInbound(row agentGatewayRowScanner) (domain.AgentGatewayInbound, error) {
	var item domain.AgentGatewayInbound
	var payload, callbackAnswer []byte
	err := row.Scan(&item.UpdateID, &item.Kind, &item.CallbackQueryID, &item.TelegramOwnerUserID,
		&item.TelegramActorUserID, &item.ChatBotID, &item.TelegramChatID, &item.TelegramMessageID, &item.CallbackData,
		&payload, &item.Attempts, &item.Status, &item.Disposition, &item.GatewayInboundID,
		&item.GatewayProfileID, &item.GatewayTaskID, &callbackAnswer, &item.LastError,
		&item.BotAPIUpdateID, &item.ReplyToMessageID, &item.TopMessageID)
	if err != nil {
		return domain.AgentGatewayInbound{}, err
	}
	item.Payload = payload
	if len(callbackAnswer) > 0 && string(callbackAnswer) != "null" {
		var answer domain.AgentGatewayCallbackAnswer
		if err := json.Unmarshal(callbackAnswer, &answer); err != nil {
			return domain.AgentGatewayInbound{}, err
		}
		item.CallbackAnswer = &answer
	}
	return item, nil
}

func (s *AgentGatewayStore) AcceptInbound(ctx context.Context, worker string, item domain.AgentGatewayInbound, ack domain.AgentGatewayInboundAck) error {
	updateID, err := strconv.ParseInt(ack.UpdateID, 10, 64)
	expectedUpdateID := item.UpdateID
	if err != nil || strconv.FormatInt(updateID, 10) != ack.UpdateID || updateID != expectedUpdateID || !ack.Accepted ||
		strings.TrimSpace(ack.GatewayInboundID) == "" || len(ack.GatewayInboundID) > 256 {
		return fmt.Errorf("invalid agent gateway acceptance response")
	}
	if err := validateGatewayAck(item.Kind, ack); err != nil {
		return err
	}
	var answer any
	if ack.Answer != nil {
		encoded, err := json.Marshal(ack.Answer)
		if err != nil {
			return err
		}
		answer = encoded
	}
	result, err := s.db.Exec(ctx, `
UPDATE agent_gateway_inbound
SET status = 'accepted', gateway_inbound_id = $4,
    gateway_profile_id = NULLIF($5, ''), gateway_task_id = NULLIF($6, ''),
    disposition = $7, callback_answer = $8::jsonb, payload = '{}'::jsonb,
    lease_owner = '', lease_until = NULL, last_error = '', updated_at = now()
WHERE update_id = $1 AND status = 'dispatching' AND lease_owner = $2 AND attempts = $3`,
		item.UpdateID, worker, item.Attempts, ack.GatewayInboundID, ack.ProfileID, ack.TaskID, ack.Disposition, answer)
	if err != nil {
		return fmt.Errorf("accept agent gateway ingress: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("accept agent gateway ingress: lease lost")
	}
	return nil
}

func validateGatewayAck(kind string, ack domain.AgentGatewayInboundAck) error {
	if len(ack.ProfileID) > 256 || len(ack.TaskID) > 256 ||
		(ack.ProfileID != "" && strings.TrimSpace(ack.ProfileID) == "") ||
		(ack.TaskID != "" && strings.TrimSpace(ack.TaskID) == "") {
		return fmt.Errorf("invalid Gateway profile or task handle")
	}
	if ack.Answer != nil && (ack.Answer.CacheTime < 0 ||
		!utf8.ValidString(ack.Answer.Text) || utf8.RuneCountInString(ack.Answer.Text) > domain.MaxBotCallbackAnswerLen ||
		!utf8.ValidString(ack.Answer.URL) || len(ack.Answer.URL) > 2048) {
		return fmt.Errorf("invalid Gateway callback answer")
	}
	if kind == "callback" {
		switch ack.Disposition {
		case "callback_accepted":
			if ack.ProfileID == "" || ack.TaskID == "" {
				return fmt.Errorf("invalid callback_accepted Gateway response")
			}
		case "callback_ignored":
			if (ack.ProfileID == "") != (ack.TaskID == "") || ack.Answer == nil {
				return fmt.Errorf("invalid callback_ignored Gateway response")
			}
		default:
			return fmt.Errorf("unknown callback Gateway disposition")
		}
		return nil
	}
	if kind == "group_message" {
		switch ack.Disposition {
		case "group_policy_updated", "group_message_ignored":
			if ack.TaskID != "" {
				return fmt.Errorf("group control acknowledgement cannot include a task")
			}
			return nil
		case "task_created":
			if ack.TaskID == "" || ack.ProfileID == "" {
				return fmt.Errorf("invalid group task_created Gateway response")
			}
			return nil
		default:
			return fmt.Errorf("unknown group Gateway disposition")
		}
	}
	switch ack.Disposition {
	case "setup_required":
		if ack.ProfileID != "" || ack.TaskID != "" {
			return fmt.Errorf("invalid setup_required Gateway response")
		}
	case "opted_in":
		if ack.ProfileID == "" || ack.TaskID != "" {
			return fmt.Errorf("invalid opted_in Gateway response")
		}
	case "capability_updated":
		if ack.ProfileID == "" || ack.TaskID != "" {
			return fmt.Errorf("invalid capability_updated Gateway response")
		}
	case "task_created":
		if ack.TaskID == "" || ack.ProfileID == "" {
			return fmt.Errorf("invalid task_created Gateway response")
		}
	default:
		return fmt.Errorf("unknown Gateway disposition")
	}
	return nil
}

func (s *AgentGatewayStore) FailInbound(ctx context.Context, worker string, item domain.AgentGatewayInbound, cause string, retryAt time.Time, retry bool) error {
	status := "failed"
	if retry {
		status = "pending"
	}
	result, err := s.db.Exec(ctx, `
UPDATE agent_gateway_inbound
SET status = $4, next_attempt_at = $5, lease_owner = '', lease_until = NULL,
    last_error = left($6, 512), updated_at = now()
WHERE update_id = $1 AND status = 'dispatching' AND lease_owner = $2 AND attempts = $3`,
		item.UpdateID, worker, item.Attempts, status, retryAt, cause)
	if err != nil {
		return fmt.Errorf("record agent gateway ingress failure: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("record agent gateway ingress failure: lease lost")
	}
	return nil
}

func (s *AgentGatewayStore) Inbound(ctx context.Context, updateID int64) (domain.AgentGatewayInbound, bool, error) {
	item, err := scanAgentGatewayInbound(s.db.QueryRow(ctx, `
SELECT update_id, kind, COALESCE(callback_query_id, 0), owner_user_id,
       telegram_actor_user_id, chat_bot_id, telegram_chat_id, telegram_message_id,
       COALESCE(callback_data, ''::bytea), payload::text, attempts, status,
       disposition, COALESCE(gateway_inbound_id, ''), COALESCE(gateway_profile_id, ''),
       COALESCE(gateway_task_id, ''), callback_answer::text, last_error,
       COALESCE(bot_api_update_id, 0), COALESCE(telegram_reply_to_message_id, 0),
       COALESCE(telegram_top_message_id, 0)
FROM agent_gateway_inbound WHERE update_id = $1`, updateID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentGatewayInbound{}, false, nil
	}
	return item, err == nil, err
}

func (s *AgentGatewayStore) InboundByGatewayID(ctx context.Context, gatewayInboundID string) (domain.AgentGatewayInbound, bool, error) {
	if strings.TrimSpace(gatewayInboundID) == "" {
		return domain.AgentGatewayInbound{}, false, nil
	}
	item, err := scanAgentGatewayInbound(s.db.QueryRow(ctx, `
SELECT update_id, kind, COALESCE(callback_query_id, 0), owner_user_id,
       telegram_actor_user_id, chat_bot_id, telegram_chat_id, telegram_message_id,
       COALESCE(callback_data, ''::bytea), payload::text, attempts, status,
       disposition, COALESCE(gateway_inbound_id, ''), COALESCE(gateway_profile_id, ''),
       COALESCE(gateway_task_id, ''), callback_answer::text, last_error,
       COALESCE(bot_api_update_id, 0), COALESCE(telegram_reply_to_message_id, 0),
       COALESCE(telegram_top_message_id, 0)
FROM agent_gateway_inbound WHERE gateway_inbound_id = $1 AND status = 'accepted'`, gatewayInboundID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentGatewayInbound{}, false, nil
	}
	return item, err == nil, err
}

func (s *AgentGatewayStore) ConsumeNonce(ctx context.Context, gatewayID, keyID, nonce string, expires time.Time) (bool, error) {
	result, err := s.db.Exec(ctx, `
INSERT INTO agent_gateway_request_nonces (gateway_id, key_id, nonce, expires_at)
VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, gatewayID, keyID, nonce, expires)
	if err != nil {
		return false, fmt.Errorf("persist agent gateway nonce: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

func (s *AgentGatewayStore) CleanupNonces(ctx context.Context, before time.Time, limit int) error {
	if limit <= 0 {
		limit = 1000
	}
	_, err := s.db.Exec(ctx, `
WITH expired AS (
    SELECT ctid FROM agent_gateway_request_nonces
    WHERE expires_at < $1 ORDER BY expires_at LIMIT $2
)
DELETE FROM agent_gateway_request_nonces n USING expired WHERE n.ctid = expired.ctid`, before, limit)
	if err != nil {
		return fmt.Errorf("cleanup agent gateway nonces: %w", err)
	}
	return nil
}

func (s *AgentGatewayStore) CreateDelivery(ctx context.Context, gatewayID string, req domain.AgentGatewayDeliveryRequest, requestHash []byte) (domain.AgentGatewayDeliveryReceipt, bool, error) {
	updateID, err := strconv.ParseInt(req.ReplyToUpdateID, 10, 64)
	if err != nil || updateID <= 0 || strconv.FormatInt(updateID, 10) != req.ReplyToUpdateID || len(requestHash) != 32 {
		return domain.AgentGatewayDeliveryReceipt{}, false, ErrAgentGatewayBinding
	}
	body, err := json.Marshal(req)
	if err != nil {
		return domain.AgentGatewayDeliveryReceipt{}, false, err
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.AgentGatewayDeliveryReceipt{}, false, fmt.Errorf("agent gateway delivery: database does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.AgentGatewayDeliveryReceipt{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var existingHash []byte
	existing, err := scanAgentGatewayDelivery(tx.QueryRow(ctx, `
SELECT receipt_id, gateway_id, idempotency_key, gateway_inbound_id,
       COALESCE(gateway_task_id, ''), COALESCE(gateway_profile_id, ''),
       telegram_owner_user_id, reply_to_update_id, purpose, operation,
       COALESCE(telegram_message_id, 0), text, COALESCE(inline_keyboard::text, 'null'),
       request_body::text, status, COALESCE(result_telegram_message_id, 0),
       attempts, next_attempt_at, last_error, request_hash
FROM agent_gateway_delivery_receipts WHERE gateway_id = $1 AND idempotency_key = $2`, gatewayID, req.IdempotencyKey))
	if err == nil {
		existingHash = existing.RequestHash
		if !equalBytes(existingHash, requestHash) {
			return domain.AgentGatewayDeliveryReceipt{}, false, ErrAgentGatewayConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.AgentGatewayDeliveryReceipt{}, false, err
		}
		return existing, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentGatewayDeliveryReceipt{}, false, err
	}

	var bindingUpdateID, botAPIUpdateID, ownerID, actorID, botID, chatID int64
	var messageID, replyToMessageID, topMessageID int
	var kind, disposition, profileID, taskID string
	err = tx.QueryRow(ctx, `
SELECT update_id, COALESCE(bot_api_update_id, 0), kind, owner_user_id, telegram_actor_user_id, chat_bot_id,
       telegram_chat_id, telegram_message_id, telegram_reply_to_message_id,
       telegram_top_message_id, disposition,
       COALESCE(gateway_profile_id, ''), COALESCE(gateway_task_id, '')
FROM agent_gateway_inbound
WHERE gateway_inbound_id = $1 AND status = 'accepted'
FOR SHARE`, req.GatewayInboundID).Scan(&bindingUpdateID, &botAPIUpdateID, &kind, &ownerID, &actorID,
		&botID, &chatID, &messageID, &replyToMessageID, &topMessageID, &disposition, &profileID, &taskID)
	stableUpdateID := bindingUpdateID
	if err != nil || botID <= 0 || ownerID != req.TelegramOwnerID ||
		stableUpdateID != updateID || (kind != "group_message" && chatID != ownerID) ||
		(kind == "group_message" && (chatID <= 0 || actorID <= 0)) ||
		!validDeliveryPurpose(req, kind, disposition, profileID, taskID) {
		return domain.AgentGatewayDeliveryReceipt{}, false, ErrAgentGatewayBinding
	}
	if req.Operation == "edit" {
		var sentMessageID int
		if req.Purpose == "group_task_update" {
			err = tx.QueryRow(ctx, `
SELECT d.result_telegram_message_id
FROM agent_gateway_delivery_receipts d
JOIN agent_gateway_inbound prior ON prior.gateway_inbound_id = d.gateway_inbound_id
WHERE d.gateway_id = $1 AND d.gateway_task_id = $2 AND d.telegram_owner_user_id = $3
  AND d.purpose = 'group_task_update' AND d.operation = 'send' AND d.status = 'delivered'
  AND prior.kind = 'group_message' AND prior.telegram_chat_id = $4
  AND prior.telegram_top_message_id = $5 AND prior.chat_bot_id = $6
FOR SHARE`, gatewayID, req.GatewayTaskID, req.TelegramOwnerID, chatID, topMessageID, botID).Scan(&sentMessageID)
		} else {
			err = tx.QueryRow(ctx, `
SELECT result_telegram_message_id
FROM agent_gateway_delivery_receipts
WHERE gateway_id = $1 AND gateway_task_id = $2 AND telegram_owner_user_id = $3
  AND purpose = 'task_update' AND operation = 'send' AND status = 'delivered'
FOR SHARE`, gatewayID, req.GatewayTaskID, req.TelegramOwnerID).Scan(&sentMessageID)
		}
		if err != nil || sentMessageID <= 0 || sentMessageID != req.TelegramMessageID {
			return domain.AgentGatewayDeliveryReceipt{}, false, ErrAgentGatewayBinding
		}
	}

	inlineJSON := []byte("null")
	if req.InlineKeyboard != nil {
		inlineJSON, err = json.Marshal(req.InlineKeyboard)
		if err != nil {
			return domain.AgentGatewayDeliveryReceipt{}, false, err
		}
	}
	row := tx.QueryRow(ctx, `
INSERT INTO agent_gateway_delivery_receipts
    (gateway_id, idempotency_key, request_hash, gateway_inbound_id, purpose,
     operation, telegram_owner_user_id, reply_to_update_id, gateway_profile_id,
     gateway_task_id, telegram_message_id, text, inline_keyboard, request_body)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),NULLIF($10,''),NULLIF($11,0),$12,$13::jsonb,$14::jsonb)
ON CONFLICT DO NOTHING
RETURNING receipt_id, gateway_id, idempotency_key, gateway_inbound_id,
          COALESCE(gateway_task_id, ''), COALESCE(gateway_profile_id, ''),
          telegram_owner_user_id, reply_to_update_id, purpose, operation,
          COALESCE(telegram_message_id, 0), text, COALESCE(inline_keyboard::text, 'null'),
          request_body::text, status, COALESCE(result_telegram_message_id, 0),
          attempts, next_attempt_at, last_error, request_hash`,
		gatewayID, req.IdempotencyKey, requestHash, req.GatewayInboundID, req.Purpose,
		req.Operation, req.TelegramOwnerID, updateID, req.GatewayProfileID,
		req.GatewayTaskID, req.TelegramMessageID, req.Text, inlineJSON, body)
	created, err := scanAgentGatewayDelivery(row)
	if errors.Is(err, pgx.ErrNoRows) {
		created, err = scanAgentGatewayDelivery(tx.QueryRow(ctx, `
SELECT receipt_id, gateway_id, idempotency_key, gateway_inbound_id,
       COALESCE(gateway_task_id, ''), COALESCE(gateway_profile_id, ''),
       telegram_owner_user_id, reply_to_update_id, purpose, operation,
       COALESCE(telegram_message_id, 0), text, COALESCE(inline_keyboard::text, 'null'),
       request_body::text, status, COALESCE(result_telegram_message_id, 0),
       attempts, next_attempt_at, last_error, request_hash
FROM agent_gateway_delivery_receipts WHERE gateway_id = $1 AND idempotency_key = $2`, gatewayID, req.IdempotencyKey))
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.AgentGatewayDeliveryReceipt{}, false, ErrAgentGatewayConflict
		}
		if err == nil && !equalBytes(created.RequestHash, requestHash) {
			return domain.AgentGatewayDeliveryReceipt{}, false, ErrAgentGatewayConflict
		}
	}
	if err != nil {
		return domain.AgentGatewayDeliveryReceipt{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.AgentGatewayDeliveryReceipt{}, false, err
	}
	return created, false, nil
}

func validDeliveryPurpose(req domain.AgentGatewayDeliveryRequest, kind, disposition, profileID, taskID string) bool {
	switch req.Purpose {
	case "setup":
		return disposition == "setup_required" && req.GatewayProfileID == "" && req.GatewayTaskID == "" && req.Operation == "send"
	case "profile_welcome":
		return (disposition == "opted_in" || disposition == "capability_updated") &&
			req.GatewayProfileID != "" && req.GatewayProfileID == profileID && req.GatewayTaskID == "" && req.Operation == "send"
	case "task_update":
		return (kind == "message" || kind == "callback") && (disposition == "task_created" || disposition == "callback_accepted") &&
			profileID != "" && req.GatewayProfileID == "" && req.GatewayTaskID != "" && req.GatewayTaskID == taskID
	case "group_task_update":
		if kind != "group_message" {
			return false
		}
		switch disposition {
		case "group_policy_updated", "group_message_ignored", "task_created":
		default:
			return false
		}
		return (req.GatewayProfileID == "" || req.GatewayProfileID == profileID) &&
			(req.GatewayTaskID == "" || req.GatewayTaskID == taskID) &&
			(req.GatewayTaskID == "" || taskID != "")
	case "owner_approval":
		return kind == "group_message" && disposition == "task_created" && req.Operation == "send" &&
			profileID != "" && taskID != "" && req.GatewayProfileID == profileID && req.GatewayTaskID == taskID
	default:
		return false
	}
}

func scanAgentGatewayDelivery(row agentGatewayRowScanner) (domain.AgentGatewayDeliveryReceipt, error) {
	var r domain.AgentGatewayDeliveryReceipt
	var inlineJSON, requestBody []byte
	err := row.Scan(&r.ReceiptID, &r.GatewayID, &r.IdempotencyKey, &r.GatewayInboundID,
		&r.GatewayTaskID, &r.GatewayProfileID, &r.TelegramOwnerID, &r.ReplyToUpdateID,
		&r.Purpose, &r.Operation, &r.TelegramMessageID, &r.Text, &inlineJSON,
		&requestBody, &r.Status, &r.ResultTelegramMessageID, &r.Attempts,
		&r.NextAttemptAt, &r.LastError, &r.RequestHash)
	if err != nil {
		return domain.AgentGatewayDeliveryReceipt{}, err
	}
	r.InlineKeyboardJSON = inlineJSON
	r.RequestBody = requestBody
	return r, nil
}

func (s *AgentGatewayStore) ClaimDeliveries(ctx context.Context, worker string, limit int, lease time.Duration) ([]domain.AgentGatewayDeliveryReceipt, error) {
	if worker == "" || limit <= 0 || lease <= 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
WITH claimable AS (
    SELECT r.receipt_id
    FROM agent_gateway_delivery_receipts r
    WHERE ((r.status = 'pending' AND r.next_attempt_at <= now())
        OR (r.status = 'dispatching' AND r.lease_until <= now()))
      AND (r.gateway_task_id IS NULL OR NOT EXISTS (
          SELECT 1 FROM agent_gateway_delivery_receipts earlier
          WHERE earlier.gateway_id = r.gateway_id
            AND earlier.gateway_task_id = r.gateway_task_id
            AND earlier.telegram_owner_user_id = r.telegram_owner_user_id
            AND earlier.receipt_id < r.receipt_id
            AND earlier.status IN ('pending', 'dispatching')
      ))
    ORDER BY r.receipt_id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE agent_gateway_delivery_receipts AS r
SET status = 'dispatching', attempts = r.attempts + 1,
    lease_owner = $2, lease_until = now() + make_interval(secs => $3::double precision),
    updated_at = now()
FROM claimable WHERE r.receipt_id = claimable.receipt_id
RETURNING r.receipt_id, r.gateway_id, r.idempotency_key, r.gateway_inbound_id,
          COALESCE(r.gateway_task_id, ''), COALESCE(r.gateway_profile_id, ''),
          r.telegram_owner_user_id, r.reply_to_update_id, r.purpose, r.operation,
          COALESCE(r.telegram_message_id, 0), r.text, COALESCE(r.inline_keyboard::text, 'null'),
          r.request_body::text, r.status, COALESCE(r.result_telegram_message_id, 0),
          r.attempts, r.next_attempt_at, r.last_error, r.request_hash`, limit, worker, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim agent gateway deliveries: %w", err)
	}
	defer rows.Close()
	var out []domain.AgentGatewayDeliveryReceipt
	for rows.Next() {
		receipt, err := scanAgentGatewayDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, receipt)
	}
	return out, rows.Err()
}

func (s *AgentGatewayStore) CompleteDelivery(ctx context.Context, worker string, receipt domain.AgentGatewayDeliveryReceipt, messageID int) error {
	if messageID <= 0 {
		return fmt.Errorf("invalid delivered Telegram message id")
	}
	result, err := s.db.Exec(ctx, `
UPDATE agent_gateway_delivery_receipts
SET status = 'delivered', result_telegram_message_id = $4,
    lease_owner = '', lease_until = NULL, last_error = '', updated_at = now()
WHERE receipt_id = $1 AND status = 'dispatching' AND lease_owner = $2 AND attempts = $3`,
		receipt.ReceiptID, worker, receipt.Attempts, messageID)
	if err != nil {
		return fmt.Errorf("complete agent gateway delivery: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("complete agent gateway delivery: lease lost")
	}
	return nil
}

func (s *AgentGatewayStore) FailDelivery(ctx context.Context, worker string, receipt domain.AgentGatewayDeliveryReceipt, cause string, retryAt time.Time, retry bool) error {
	status := "failed"
	if retry {
		status = "pending"
	}
	result, err := s.db.Exec(ctx, `
UPDATE agent_gateway_delivery_receipts
SET status = $4, next_attempt_at = $5, lease_owner = '', lease_until = NULL,
    last_error = left($6, 512), updated_at = now()
WHERE receipt_id = $1 AND status = 'dispatching' AND lease_owner = $2 AND attempts = $3`,
		receipt.ReceiptID, worker, receipt.Attempts, status, retryAt, cause)
	if err != nil {
		return fmt.Errorf("record agent gateway delivery failure: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("record agent gateway delivery failure: lease lost")
	}
	return nil
}

func (s *AgentGatewayStore) DeliveryReceipt(ctx context.Context, gatewayID string, receiptID int64) (domain.AgentGatewayDeliveryReceipt, bool, error) {
	row, err := scanAgentGatewayDelivery(s.db.QueryRow(ctx, `
SELECT receipt_id, gateway_id, idempotency_key, gateway_inbound_id,
       COALESCE(gateway_task_id, ''), COALESCE(gateway_profile_id, ''),
       telegram_owner_user_id, reply_to_update_id, purpose, operation,
       COALESCE(telegram_message_id, 0), text, COALESCE(inline_keyboard::text, 'null'),
       request_body::text, status, COALESCE(result_telegram_message_id, 0),
       attempts, next_attempt_at, last_error, request_hash
FROM agent_gateway_delivery_receipts WHERE gateway_id = $1 AND receipt_id = $2`, gatewayID, receiptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentGatewayDeliveryReceipt{}, false, nil
	}
	return row, err == nil, err
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
