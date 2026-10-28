package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// MessageStore 用 PostgreSQL 实现 store.MessageStore。
type MessageStore struct {
	db                      sqlcgen.DBTX
	q                       *sqlcgen.Queries
	boxIDs                  store.BoxIDAllocator
	privateSendLanes        *userLaneActor
	log                     *zap.Logger
	agentGatewayInboundHook func(context.Context, pgx.Tx, domain.AgentGatewayInbound) error
}

type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// MessageStoreOption 调整 PostgreSQL MessageStore 依赖。
type MessageStoreOption func(*MessageStore)

// WithMessageAllocators 注入 Redis-backed box_id allocator。
func WithMessageAllocators(boxIDs store.BoxIDAllocator) MessageStoreOption {
	return func(s *MessageStore) {
		s.boxIDs = boxIDs
	}
}

// WithMessageLogger 注入消息 store 日志器，用于追踪消息与 pts 原子写入。
func WithMessageLogger(log *zap.Logger) MessageStoreOption {
	return func(s *MessageStore) {
		s.log = log
	}
}

// WithAgentGatewayInboundHook persists owner-authenticated Agent Bot text in
// the Gateway ingress outbox inside the same transaction as both
// private-message projections. Other private messages remain on the ordinary
// hot path.
func WithAgentGatewayInboundHook(hook func(context.Context, pgx.Tx, domain.AgentGatewayInbound) error) MessageStoreOption {
	return func(s *MessageStore) {
		s.agentGatewayInboundHook = hook
	}
}

// NewMessageStore 基于 pgx 连接池（或事务）创建 MessageStore。
func NewMessageStore(db sqlcgen.DBTX, opts ...MessageStoreOption) *MessageStore {
	s := &MessageStore{db: db, q: sqlcgen.New(db), privateSendLanes: defaultPrivateSendLaneActor}
	for _, opt := range opts {
		opt(s)
	}
	if s.log == nil {
		s.log = zap.NewNop()
	}
	if s.boxIDs == nil {
		s.boxIDs = pgBoxIDAllocator{s: s}
	}
	return s
}
