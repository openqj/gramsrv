package agentgateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

func (h *deliveryHandler) authorizeGroup(w http.ResponseWriter, r *http.Request, body []byte) {
	var request domain.AgentGatewayGroupAuthorizationRequest
	if err := decodeStrict(body, &request); err != nil || !validGroupAuthorizationRequest(request) {
		http.Error(w, "invalid group authorization request", http.StatusBadRequest)
		return
	}
	response, err := h.service.authorizeGroup(r, request)
	if err != nil {
		h.service.logger.Warn("check live Agent Gateway group authorization", zap.Error(err))
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func validGroupAuthorizationRequest(request domain.AgentGatewayGroupAuthorizationRequest) bool {
	if request.SchemaVersion != 1 || request.ChatType != "channel" || request.ChatID <= 0 ||
		request.BotUserID <= 0 || request.ActorUserID <= 0 || request.OwnerUserID <= 0 {
		return false
	}
	switch request.Action {
	case "task", "delivery", "set_collaboration":
		return request.TargetUserID == 0 && request.TargetBotUserID == 0
	case "collaborate":
		return request.TargetUserID > 0 && request.TargetBotUserID > 0 &&
			request.TargetBotUserID != request.BotUserID
	default:
		return false
	}
}

func (s *Service) authorizeGroup(r *http.Request, request domain.AgentGatewayGroupAuthorizationRequest) (domain.AgentGatewayGroupAuthorizationResponse, error) {
	response := domain.AgentGatewayGroupAuthorizationResponse{
		SchemaVersion: 1, Action: request.Action, Reason: "source_bot_owner_mismatch", CheckedAt: time.Now().Unix(),
		OwnerMemberStatus: "not_checked", TargetMemberStatus: "not_checked", TargetRole: "not_checked",
		TargetBotMemberStatus: "not_checked", TargetBotRole: "not_checked", BotMemberStatus: "not_checked",
	}
	profile, found, err := s.botProfiles.BotInfo(r.Context(), request.BotUserID)
	if err != nil {
		return response, err
	}
	if !found || profile.BotUserID != request.BotUserID || profile.OwnerUserID != request.OwnerUserID {
		return response, nil
	}
	optIn, ok := s.store.(agentBotOptInChecker)
	if !ok {
		response.Reason = "bot_not_opted_in"
		return response, nil
	}
	active, err := optIn.AgentBotOptedIn(r.Context(), request.BotUserID, request.OwnerUserID)
	if err != nil {
		return response, err
	}
	if !active {
		response.Reason = "bot_not_opted_in"
		return response, nil
	}
	response.Reason = "actor_not_member"
	actor, err := s.groupAuthority.GetParticipant(r.Context(), request.ActorUserID, request.ChatID, request.ActorUserID)
	if err != nil {
		if noGroupMembership(err) {
			response.ActorMemberStatus = "not_member"
			return response, nil
		}
		return response, err
	}
	response.ActorMemberStatus, response.ActorRole = string(actor.Status), string(actor.Role)
	if actor.Status != domain.ChannelMemberActive {
		return response, nil
	}
	botView, err := s.groupAuthority.GetChannel(r.Context(), request.BotUserID, request.ChatID)
	if err != nil {
		if noGroupMembership(err) {
			response.BotMemberStatus = "not_member"
			response.Reason = "bot_not_member"
			return response, nil
		}
		return response, err
	}
	response.BotMemberStatus = string(botView.Self.Status)
	response.BotCanSendMessage = botCanSendGroupMessage(botView)
	if !botView.Channel.Megagroup || botView.Channel.Broadcast {
		response.Reason = "unsupported_group_type"
		return response, nil
	}
	if botView.Self.Status != domain.ChannelMemberActive {
		response.Reason = "bot_not_member"
		return response, nil
	}
	if (request.Action == "task" || request.Action == "delivery") && !response.BotCanSendMessage {
		response.Reason = "bot_cannot_send"
		return response, nil
	}
	if request.Action == "collaborate" {
		targetProfile, targetProfileFound, targetProfileErr := s.botProfiles.BotInfo(r.Context(), request.TargetBotUserID)
		if targetProfileErr != nil {
			return response, targetProfileErr
		}
		if !targetProfileFound || targetProfile.BotUserID != request.TargetBotUserID || targetProfile.OwnerUserID != request.TargetUserID {
			response.Reason = "target_bot_owner_mismatch"
			return response, nil
		}
		targetOptIn, ok := s.store.(agentBotOptInChecker)
		if !ok {
			response.Reason = "target_bot_not_opted_in"
			return response, nil
		}
		targetOptedIn, targetOptInErr := targetOptIn.AgentBotOptedIn(r.Context(), request.TargetBotUserID, request.TargetUserID)
		if targetOptInErr != nil {
			return response, targetOptInErr
		}
		if !targetOptedIn {
			response.Reason = "target_bot_not_opted_in"
			return response, nil
		}
		targetBot, targetErr := s.groupAuthority.GetChannel(r.Context(), request.TargetBotUserID, request.ChatID)
		if targetErr != nil {
			if noGroupMembership(targetErr) {
				response.TargetBotMemberStatus = "not_member"
				response.Reason = "target_bot_not_member"
				return response, nil
			}
			return response, targetErr
		}
		response.TargetBotMemberStatus = string(targetBot.Self.Status)
		response.TargetBotCanSendMessage = botCanSendGroupMessage(targetBot)
		response.TargetBotRole = string(targetBot.Self.Role)
		if !targetBot.Channel.Megagroup || targetBot.Channel.Broadcast {
			response.Reason = "unsupported_group_type"
			return response, nil
		}
		if targetBot.Self.Status != domain.ChannelMemberActive {
			response.Reason = "target_bot_not_member"
			return response, nil
		}
	}
	if request.Action == "set_collaboration" &&
		actor.Role != domain.ChannelRoleCreator &&
		!(actor.Role == domain.ChannelRoleAdmin && actor.AdminRights.ChangeInfo) {
		response.Reason = "change_info_required"
		return response, nil
	}
	response.Allowed = true
	response.Reason = "allowed"
	return response, nil
}

func botCanSendGroupMessage(view domain.ChannelView) bool {
	channel, member := view.Channel, view.Self
	if !channel.Megagroup || channel.Broadcast || member.Status != domain.ChannelMemberActive {
		return false
	}
	if member.Role == domain.ChannelRoleCreator || member.Role == domain.ChannelRoleAdmin {
		return true
	}
	if domain.ChannelBannedRightsBlockMessage(domain.SendChannelMessageRequest{
		UserID: member.UserID, ChannelID: channel.ID, Message: "Agent Gateway message",
	}, channel, member, view.SelfBoostsApplied) {
		return false
	}
	if member.BannedRights.SendMessages {
		return false
	}
	if !channel.DefaultBannedRights.SendMessages {
		return true
	}
	return channel.BoostsUnrestrict > 0 && view.SelfBoostsApplied >= channel.BoostsUnrestrict
}

func noGroupMembership(err error) bool {
	return errors.Is(err, domain.ErrUserNotParticipant) || errors.Is(err, domain.ErrChannelPrivate) ||
		errors.Is(err, domain.ErrChannelInvalid)
}
