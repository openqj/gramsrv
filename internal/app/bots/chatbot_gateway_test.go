package bots

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestChatBotDoesNotRunLocalPrivateResponder(t *testing.T) {
	service, users, _, messages := newTestService(t)
	owner := newOwner(t, users, "+15550003")
	service.OnPrivateMessage(context.Background(), domain.ChatBotUserID, domain.Message{
		From: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID},
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: domain.ChatBotUserID},
		Body: "/start",
	}, domain.ClientSessionMetadata{})

	replies, err := messages.ListByUser(context.Background(), owner.ID, domain.MessageFilter{
		HasPeer: true,
		Peer:    domain.Peer{Type: domain.PeerTypeUser, ID: domain.ChatBotUserID},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("list ChatBot conversation: %v", err)
	}
	if len(replies.Messages) != 0 {
		t.Fatalf("local ChatBot responder wrote %d messages", len(replies.Messages))
	}
}
