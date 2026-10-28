package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/domain"
)

// PrivateAgentSessionTitleUpdatePublisher sends an absolute name update for a
// reserved Agent peer. It never resolves or mutates a real user profile.
type PrivateAgentSessionTitleUpdatePublisher struct {
	Sessions SessionBinder
}

func (p PrivateAgentSessionTitleUpdatePublisher) PushSessionTitle(ctx context.Context, ownerUserID, peerID int64, title string) error {
	if p.Sessions == nil || ownerUserID <= 0 || !domain.IsAgentCompatibilityPeerID(peerID) || title == "" {
		return fmt.Errorf("invalid private Agent session title update target")
	}
	user := &tg.User{ID: peerID, FirstName: title}
	update := &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateUserName{
			UserID:    peerID,
			FirstName: title,
			Usernames: tgUsernames(""),
		}},
		Users: []tg.UserClass{user},
		Date:  int(time.Now().Unix()),
	}
	_, err := p.Sessions.PushToUserExceptAuthKeySession(ctx, ownerUserID, [8]byte{}, 0, proto.MessageFromServer, update)
	return err
}
