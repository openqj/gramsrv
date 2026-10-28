package rpc

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/domain"
)

func (r *Router) privateAgentDialogs(ctx context.Context, req *tg.MessagesGetDialogsRequest, folderID int) (any, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil || userID <= 0 {
		return nil, internalErr()
	}
	if folderID == -3 {
		if r.deps.PrivateAgents == nil {
			return nil, internalErr()
		}
		key, err := privateAgentCreateIdempotencyKey(userID, req.Hash)
		if err != nil {
			return nil, internalErr()
		}
		session, err := r.deps.PrivateAgents.CreateConversation(ctx, userID, key)
		if err != nil || session.OwnerUserID != userID || !domain.IsAgentCompatibilityPeerID(session.PeerID) || session.GatewaySessionID == "" {
			return nil, internalErr()
		}
		return r.tgMessagesDialogs(ctx, userID, domain.DialogList{
			Count: 1,
			Dialogs: []domain.Dialog{{
				Peer:       domain.Peer{Type: domain.PeerTypeUser, ID: session.PeerID},
				AgentTitle: session.Title,
				FolderID:   -2,
			}},
		}), nil
	}
	if folderID != -2 {
		return nil, folderIDInvalidErr()
	}
	if r.deps.PrivateAgents == nil {
		return r.tgMessagesDialogs(ctx, userID, domain.DialogList{}), nil
	}
	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	offsetPeerID := int64(0)
	if _, empty := req.OffsetPeer.(*tg.InputPeerEmpty); !empty && !inputPeerClassNil(req.OffsetPeer) {
		offsetPeer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.OffsetPeer)
		if err != nil || offsetPeer.Type != domain.PeerTypeUser || !domain.IsAgentCompatibilityPeerID(offsetPeer.ID) {
			return nil, peerIDInvalidErr()
		}
		offsetPeerID = offsetPeer.ID
	} else if req.OffsetID > 0 {
		session, found, err := r.deps.PrivateAgents.ResolveConversationByMessageID(ctx, userID, req.OffsetID)
		if err != nil || !found {
			return nil, peerIDInvalidErr()
		}
		offsetPeerID = session.PeerID
	}
	sessions, total, err := r.deps.PrivateAgents.ListConversations(ctx, userID, offsetPeerID, limit)
	if err != nil {
		return nil, internalErr()
	}
	list := domain.DialogList{Dialogs: make([]domain.Dialog, 0, len(sessions)), Messages: make([]domain.Message, 0, len(sessions)), Count: len(sessions)}
	for _, session := range sessions {
		if session.OwnerUserID != userID || !domain.IsAgentCompatibilityPeerID(session.PeerID) || session.GatewaySessionID == "" {
			continue
		}
		dialog := domain.Dialog{
			Peer:       domain.Peer{Type: domain.PeerTypeUser, ID: session.PeerID},
			AgentTitle: session.Title,
			FolderID:   -2,
		}
		if r.deps.Messages != nil {
			history, historyErr := r.deps.Messages.GetHistory(ctx, userID, domain.MessageFilter{
				HasPeer: true,
				Peer:    dialog.Peer,
				Limit:   1,
			})
			if historyErr != nil {
				return nil, internalErr()
			}
			if len(history.Messages) > 0 {
				message := history.Messages[0]
				dialog.TopMessage = message.ID
				dialog.TopMessageDate = message.Date
				list.Messages = append(list.Messages, message)
			}
		}
		list.Dialogs = append(list.Dialogs, dialog)
	}
	list.Count = total
	return r.tgMessagesDialogs(ctx, userID, list), nil
}

func privateAgentCreateIdempotencyKey(ownerUserID, requestKey int64) (string, error) {
	if ownerUserID <= 0 {
		return "", fmt.Errorf("private Agent create has no authenticated owner")
	}
	if requestKey != 0 {
		return fmt.Sprintf("mtproto:%d:%d", ownerUserID, requestKey), nil
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("mtproto:%d:%s", ownerUserID, base64.RawURLEncoding.EncodeToString(nonce[:])), nil
}

func (r *Router) sendPrivateAgentMessage(ctx context.Context, userID int64, peer domain.Peer, req *tg.MessagesSendMessageRequest, entities []tg.MessageEntityClass, fingerprint []byte) (tg.UpdatesClass, bool, error) {
	if r.deps.PrivateAgents == nil || peer.Type != domain.PeerTypeUser || !domain.IsAgentCompatibilityPeerID(peer.ID) {
		return nil, false, peerIDInvalidErr()
	}
	input, ok := req.Peer.(*tg.InputPeerUser)
	if !ok || input.AccessHash != 0 {
		return nil, false, peerIDInvalidErr()
	}
	if req.RichMessage != nil || req.Message == "" || req.QuickReplyShortcut != nil || req.ScheduleDate != 0 || req.ScheduleRepeatPeriod != 0 ||
		req.ReplyMarkup != nil || req.Effect != 0 || req.AllowPaidStars != 0 {
		return nil, false, mediaInvalidErr()
	}
	if _, found, err := r.deps.PrivateAgents.ResolveConversation(ctx, userID, peer.ID); err != nil {
		return nil, false, internalErr()
	} else if !found {
		return nil, false, peerIDInvalidErr()
	}
	if err := r.checkSendRateLimit(ctx, userID, 1); err != nil {
		return nil, false, err
	}
	replyTo, err := r.messageReplyFromInput(ctx, userID, peer, req.ReplyTo)
	if err != nil {
		return nil, false, err
	}
	sessionID, _ := SessionIDFrom(ctx)
	result, _, err := r.deps.PrivateAgents.SendMessage(ctx, userID, peer.ID, domain.SendPrivateTextRequest{
		SenderUserID:           userID,
		RecipientUserID:        userID,
		RandomID:               req.RandomID,
		Message:                req.Message,
		Entities:               domainMessageEntities(entities),
		ReplyTo:                replyTo,
		Silent:                 req.Silent,
		NoForwards:             req.Noforwards,
		Date:                   int(r.clock.Now().Unix()),
		IdempotencyFingerprint: fingerprint,
		OriginAuthKeyID:        rawAuthKeyIDForOrigin(ctx),
		OriginSessionID:        sessionID,
		OriginClientSession:    clientSessionMetadataFromContext(ctx),
	})
	if err != nil {
		return nil, false, messageSendErr(err)
	}
	if req.ClearDraft {
		r.clearDraftAfterSend(ctx, userID, peer, replyTo)
	}
	users := []tg.UserClass{&tg.UserEmpty{ID: peer.ID}}
	return tgPrivateSendResultUpdates(result, req.RandomID, true, users, nil), result.Duplicate, nil
}
