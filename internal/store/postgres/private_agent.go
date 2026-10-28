package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

type PrivateAgentStore struct{ db sqlcgen.DBTX }

func privateMessageFromPeer(peerType string, peerID, fromUserID int64, outgoing bool) domain.Peer {
	if domain.PeerType(peerType) == domain.PeerTypeUser && !outgoing && domain.IsAgentCompatibilityPeerID(peerID) {
		return domain.Peer{Type: domain.PeerTypeUser, ID: peerID}
	}
	return domain.Peer{Type: domain.PeerTypeUser, ID: fromUserID}
}

func NewPrivateAgentStore(db sqlcgen.DBTX) *PrivateAgentStore {
	return &PrivateAgentStore{db: db}
}

func (s *PrivateAgentStore) CreatePrivateAgentSession(ctx context.Context, ownerUserID int64, gatewaySessionID, idempotencyKey string) (domain.PrivateAgentSession, error) {
	if ownerUserID <= 0 || gatewaySessionID == "" || idempotencyKey == "" {
		return domain.PrivateAgentSession{}, fmt.Errorf("create private Agent session: invalid owner or Gateway identity")
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.PrivateAgentSession{}, fmt.Errorf("create private Agent session: database does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.PrivateAgentSession{}, fmt.Errorf("begin private Agent session mapping: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var current domain.PrivateAgentSession
	err = tx.QueryRow(ctx, `
SELECT owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title
FROM private_agent_sessions
WHERE owner_user_id = $1 AND create_idempotency_key = $2`, ownerUserID, idempotencyKey).
		Scan(&current.OwnerUserID, &current.PeerID, &current.GatewaySessionID, &current.CreateIdempotencyKey, &current.AfterVersion, &current.Title)
	if err == nil {
		if current.GatewaySessionID != gatewaySessionID {
			return domain.PrivateAgentSession{}, fmt.Errorf("private Agent create idempotency key was reused for a different session")
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.PrivateAgentSession{}, fmt.Errorf("commit private Agent session replay: %w", err)
		}
		return current, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.PrivateAgentSession{}, fmt.Errorf("resolve private Agent create idempotency key: %w", err)
	}
	err = tx.QueryRow(ctx, `
INSERT INTO private_agent_sessions (owner_user_id, gateway_session_id, create_idempotency_key)
VALUES ($1, $2, $3)
ON CONFLICT (owner_user_id, gateway_session_id) DO NOTHING
	RETURNING owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title`,
		ownerUserID, gatewaySessionID, idempotencyKey).
		Scan(&current.OwnerUserID, &current.PeerID, &current.GatewaySessionID, &current.CreateIdempotencyKey, &current.AfterVersion, &current.Title)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
SELECT owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title
FROM private_agent_sessions
WHERE owner_user_id = $1 AND gateway_session_id = $2`, ownerUserID, gatewaySessionID).
			Scan(&current.OwnerUserID, &current.PeerID, &current.GatewaySessionID, &current.CreateIdempotencyKey, &current.AfterVersion, &current.Title)
		if err == nil && current.CreateIdempotencyKey != idempotencyKey {
			err = fmt.Errorf("Gateway session is already mapped to another create idempotency key")
		}
	}
	if err != nil {
		return domain.PrivateAgentSession{}, fmt.Errorf("persist private Agent session mapping: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.PrivateAgentSession{}, fmt.Errorf("commit private Agent session mapping: %w", err)
	}
	return current, nil
}

func (s *PrivateAgentStore) GetPrivateAgentSessionByPeer(ctx context.Context, ownerUserID, peerID int64) (domain.PrivateAgentSession, bool, error) {
	if ownerUserID <= 0 || !domain.IsAgentCompatibilityPeerID(peerID) {
		return domain.PrivateAgentSession{}, false, nil
	}
	var session domain.PrivateAgentSession
	err := s.db.QueryRow(ctx, `
SELECT owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title
FROM private_agent_sessions
WHERE owner_user_id = $1 AND peer_id = $2`, ownerUserID, peerID).
		Scan(&session.OwnerUserID, &session.PeerID, &session.GatewaySessionID, &session.CreateIdempotencyKey, &session.AfterVersion, &session.Title)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PrivateAgentSession{}, false, nil
	}
	if err != nil {
		return domain.PrivateAgentSession{}, false, fmt.Errorf("get private Agent session by peer: %w", err)
	}
	return session, true, nil
}

func (s *PrivateAgentStore) GetPrivateAgentSessionByMessageID(ctx context.Context, ownerUserID int64, messageID int) (domain.PrivateAgentSession, bool, error) {
	if ownerUserID <= 0 || messageID <= 0 {
		return domain.PrivateAgentSession{}, false, nil
	}
	var session domain.PrivateAgentSession
	err := s.db.QueryRow(ctx, `
SELECT s.owner_user_id, s.peer_id, s.gateway_session_id, s.create_idempotency_key, s.after_version, s.title
FROM private_agent_sessions s
JOIN private_agent_message_links l
  ON l.owner_user_id = s.owner_user_id AND l.gateway_session_id = s.gateway_session_id
WHERE s.owner_user_id = $1 AND l.message_box_id = $2`, ownerUserID, messageID).
		Scan(&session.OwnerUserID, &session.PeerID, &session.GatewaySessionID, &session.CreateIdempotencyKey, &session.AfterVersion, &session.Title)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PrivateAgentSession{}, false, nil
	}
	if err != nil {
		return domain.PrivateAgentSession{}, false, fmt.Errorf("get private Agent session by message: %w", err)
	}
	return session, true, nil
}

func (s *PrivateAgentStore) GetPrivateAgentSessionByGatewayID(ctx context.Context, ownerUserID int64, gatewaySessionID string) (domain.PrivateAgentSession, bool, error) {
	if ownerUserID <= 0 || gatewaySessionID == "" {
		return domain.PrivateAgentSession{}, false, nil
	}
	var session domain.PrivateAgentSession
	err := s.db.QueryRow(ctx, `
SELECT owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title
FROM private_agent_sessions
WHERE owner_user_id = $1 AND gateway_session_id = $2`, ownerUserID, gatewaySessionID).
		Scan(&session.OwnerUserID, &session.PeerID, &session.GatewaySessionID, &session.CreateIdempotencyKey, &session.AfterVersion, &session.Title)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PrivateAgentSession{}, false, nil
	}
	if err != nil {
		return domain.PrivateAgentSession{}, false, fmt.Errorf("get private Agent session by Gateway ID: %w", err)
	}
	return session, true, nil
}

func (s *PrivateAgentStore) SetPrivateAgentSessionTitle(ctx context.Context, ownerUserID int64, gatewaySessionID, eventID, title string) (domain.PrivateAgentSession, bool, error) {
	if ownerUserID <= 0 || gatewaySessionID == "" || eventID == "" || title == "" {
		return domain.PrivateAgentSession{}, false, fmt.Errorf("set private Agent session title: invalid owner/session/event/title")
	}
	var session domain.PrivateAgentSession
	err := s.db.QueryRow(ctx, `
UPDATE private_agent_sessions
SET title = $4, title_event_id = $3, title_update_delivered = FALSE
WHERE owner_user_id = $1 AND gateway_session_id = $2 AND title = ''
	RETURNING owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title`,
		ownerUserID, gatewaySessionID, eventID, title).
		Scan(&session.OwnerUserID, &session.PeerID, &session.GatewaySessionID, &session.CreateIdempotencyKey, &session.AfterVersion, &session.Title)
	if err == nil {
		return session, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.PrivateAgentSession{}, false, fmt.Errorf("set private Agent session title: %w", err)
	}
	var titleEventID string
	var delivered bool
	err = s.db.QueryRow(ctx, `
SELECT owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title, title_event_id, title_update_delivered
FROM private_agent_sessions
WHERE owner_user_id = $1 AND gateway_session_id = $2`, ownerUserID, gatewaySessionID).
		Scan(&session.OwnerUserID, &session.PeerID, &session.GatewaySessionID, &session.CreateIdempotencyKey, &session.AfterVersion, &session.Title, &titleEventID, &delivered)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PrivateAgentSession{}, false, domain.ErrPrivateAgentSessionNotFound
	}
	if err != nil {
		return domain.PrivateAgentSession{}, false, fmt.Errorf("read private Agent session title: %w", err)
	}
	if titleEventID != eventID {
		return session, false, nil
	}
	if session.Title != title {
		return domain.PrivateAgentSession{}, false, fmt.Errorf("private Agent title event was replayed with a different title")
	}
	return session, !delivered, nil
}

func (s *PrivateAgentStore) MarkPrivateAgentSessionTitleUpdateDelivered(ctx context.Context, ownerUserID int64, gatewaySessionID, eventID string) error {
	if ownerUserID <= 0 || gatewaySessionID == "" || eventID == "" {
		return fmt.Errorf("mark private Agent session title delivered: invalid owner/session/event")
	}
	tag, err := s.db.Exec(ctx, `
UPDATE private_agent_sessions
SET title_update_delivered = TRUE
WHERE owner_user_id = $1 AND gateway_session_id = $2 AND title_event_id = $3 AND title <> ''`,
		ownerUserID, gatewaySessionID, eventID)
	if err != nil {
		return fmt.Errorf("mark private Agent session title delivered: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrPrivateAgentSessionNotFound
	}
	return nil
}

func (s *PrivateAgentStore) ListPrivateAgentSessions(ctx context.Context, ownerUserID, offsetPeerID int64, limit int) ([]domain.PrivateAgentSession, int, error) {
	if ownerUserID <= 0 {
		return nil, 0, fmt.Errorf("list private Agent sessions: missing owner")
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	var total int
	if err := s.db.QueryRow(ctx, `SELECT count(*) FROM private_agent_sessions WHERE owner_user_id = $1`, ownerUserID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count private Agent sessions: %w", err)
	}
	rows, err := s.db.Query(ctx, `
	WITH ordered AS (
	    SELECT owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title,
	           row_number() OVER (ORDER BY last_activity_at DESC, created_at DESC, peer_id DESC) AS position
	    FROM private_agent_sessions
	    WHERE owner_user_id = $1
	), offset_row AS (
	    SELECT position FROM ordered WHERE peer_id = NULLIF($2, 0)
	)
	SELECT owner_user_id, peer_id, gateway_session_id, create_idempotency_key, after_version, title
	FROM ordered
	WHERE $2 = 0 OR position > COALESCE((SELECT position FROM offset_row), 0)
	ORDER BY position
	LIMIT $3`, ownerUserID, offsetPeerID, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("list private Agent sessions: %w", err)
	}
	defer rows.Close()
	var out []domain.PrivateAgentSession
	for rows.Next() {
		var session domain.PrivateAgentSession
		if err := rows.Scan(&session.OwnerUserID, &session.PeerID, &session.GatewaySessionID, &session.CreateIdempotencyKey, &session.AfterVersion, &session.Title); err != nil {
			return nil, 0, fmt.Errorf("scan private Agent session: %w", err)
		}
		out = append(out, session)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate private Agent sessions: %w", err)
	}
	return out, total, nil
}

func (s *PrivateAgentStore) RecordPrivateAgentTask(ctx context.Context, ownerUserID int64, gatewaySessionID, taskID, idempotencyKey string) error {
	if ownerUserID <= 0 || gatewaySessionID == "" || taskID == "" {
		return fmt.Errorf("record private Agent task: invalid owner/session/task")
	}
	tag, err := s.db.Exec(ctx, `
INSERT INTO private_agent_tasks (owner_user_id, gateway_session_id, gateway_task_id, idempotency_key)
SELECT $1, $2, $3, NULLIF($4, '')
FROM private_agent_sessions
WHERE owner_user_id = $1 AND gateway_session_id = $2
ON CONFLICT (owner_user_id, gateway_session_id, gateway_task_id)
DO UPDATE SET idempotency_key = COALESCE(private_agent_tasks.idempotency_key, EXCLUDED.idempotency_key)
WHERE private_agent_tasks.idempotency_key IS NULL OR EXCLUDED.idempotency_key IS NULL OR
      private_agent_tasks.idempotency_key = EXCLUDED.idempotency_key`, ownerUserID, gatewaySessionID, taskID, idempotencyKey)
	if err != nil {
		return fmt.Errorf("record private Agent task: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("record private Agent task: session not found or idempotency key conflicts")
	}
	return nil
}

func (s *PrivateAgentStore) InsertPrivateAgentMessageLink(ctx context.Context, ownerUserID int64, gatewaySessionID, taskID, eventID string, messageBoxID int, role, eventType, status string, version int64, payload []byte) error {
	if ownerUserID <= 0 || gatewaySessionID == "" || taskID == "" || eventID == "" || messageBoxID <= 0 ||
		(role != "user" && role != "assistant") || version < 0 {
		return fmt.Errorf("insert private Agent message link: invalid input")
	}
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	tag, err := s.db.Exec(ctx, `
INSERT INTO private_agent_message_links
    (owner_user_id, gateway_session_id, peer_id, message_box_id, gateway_task_id, event_id, role, event_type, status, version, payload)
SELECT s.owner_user_id, s.gateway_session_id, s.peer_id, $5, $3, $4, $6, $7, $8, $9, $10::jsonb
FROM private_agent_sessions s
JOIN private_agent_tasks t
  ON t.owner_user_id = s.owner_user_id AND t.gateway_session_id = s.gateway_session_id AND t.gateway_task_id = $3
WHERE s.owner_user_id = $1 AND s.gateway_session_id = $2
ON CONFLICT (owner_user_id, gateway_session_id, event_id) DO NOTHING`,
		ownerUserID, gatewaySessionID, taskID, eventID, messageBoxID, role, eventType, status, version, payload)
	if err != nil {
		return fmt.Errorf("insert private Agent message link: %w", err)
	}
	if tag.RowsAffected() == 1 {
		_, err = s.db.Exec(ctx, `UPDATE private_agent_sessions SET last_activity_at = now() WHERE owner_user_id = $1 AND gateway_session_id = $2`, ownerUserID, gatewaySessionID)
	}
	return err
}

func (s *PrivateAgentStore) PrivateAgentTaskMessageID(ctx context.Context, ownerUserID int64, gatewaySessionID, taskID string) (int, bool, error) {
	var id int
	err := s.db.QueryRow(ctx, `
SELECT message_box_id
FROM private_agent_message_links
WHERE owner_user_id = $1 AND gateway_session_id = $2 AND gateway_task_id = $3 AND role = 'user'
ORDER BY message_box_id
LIMIT 1`, ownerUserID, gatewaySessionID, taskID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get private Agent task prompt message: %w", err)
	}
	return id, true, nil
}

func (s *PrivateAgentStore) SetPrivateAgentCursor(ctx context.Context, ownerUserID int64, gatewaySessionID string, afterVersion int64) error {
	if ownerUserID <= 0 || gatewaySessionID == "" || afterVersion < 0 {
		return fmt.Errorf("set private Agent snapshot cursor: invalid input")
	}
	tag, err := s.db.Exec(ctx, `
UPDATE private_agent_sessions
SET after_version = GREATEST(after_version, $3)
WHERE owner_user_id = $1 AND gateway_session_id = $2`, ownerUserID, gatewaySessionID, afterVersion)
	if err != nil {
		return fmt.Errorf("set private Agent snapshot cursor: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("set private Agent snapshot cursor: session not found")
	}
	return nil
}
