package postgres

import (
	"testing"

	"telesrv/internal/domain"
)

func TestValidateGatewayAckDispositionByKind(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		ack  domain.AgentGatewayInboundAck
		want bool
	}{
		{name: "message setup", kind: "message", ack: domain.AgentGatewayInboundAck{Disposition: "setup_required"}, want: true},
		{name: "message opted in", kind: "message", ack: domain.AgentGatewayInboundAck{Disposition: "opted_in", ProfileID: "p1"}, want: true},
		{name: "capability updated", kind: "message", ack: domain.AgentGatewayInboundAck{Disposition: "capability_updated", ProfileID: "p1"}, want: true},
		{name: "message task", kind: "message", ack: domain.AgentGatewayInboundAck{Disposition: "task_created", ProfileID: "p1", TaskID: "t1"}, want: true},
		{name: "task missing profile", kind: "message", ack: domain.AgentGatewayInboundAck{Disposition: "task_created", TaskID: "t1"}},
		{name: "valid callback", kind: "callback", ack: domain.AgentGatewayInboundAck{Disposition: "callback_accepted", ProfileID: "p1", TaskID: "t1"}, want: true},
		{name: "expired callback answer", kind: "callback", ack: domain.AgentGatewayInboundAck{Disposition: "callback_ignored", Answer: &domain.AgentGatewayCallbackAnswer{Text: "Expired"}}, want: true},
		{name: "ignored callback without answer", kind: "callback", ack: domain.AgentGatewayInboundAck{Disposition: "callback_ignored"}},
		{name: "callback cannot create task", kind: "callback", ack: domain.AgentGatewayInboundAck{Disposition: "task_created", ProfileID: "p1", TaskID: "t1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateGatewayAck(tc.kind, tc.ack)
			if (err == nil) != tc.want {
				t.Fatalf("validateGatewayAck() error = %v, want success %v", err, tc.want)
			}
		})
	}
}

func TestTaskDeliveryMayBindToAcceptedCallback(t *testing.T) {
	req := domain.AgentGatewayDeliveryRequest{
		GatewayTaskID: "task-1", TelegramOwnerID: 99, Purpose: "task_update", Operation: "edit",
	}
	if !validDeliveryPurpose(req, "callback", "callback_accepted", "profile-1", "task-1") {
		t.Fatal("task update bound to an accepted callback was rejected")
	}
	if validDeliveryPurpose(req, "callback", "callback_ignored", "profile-1", "task-1") {
		t.Fatal("task update bound to an ignored callback was accepted")
	}
	if validDeliveryPurpose(req, "callback", "callback_accepted", "profile-1", "other-task") {
		t.Fatal("task update bound to a different task was accepted")
	}
	profileWelcome := domain.AgentGatewayDeliveryRequest{GatewayProfileID: "profile-1", TelegramOwnerID: 99, Purpose: "profile_welcome", Operation: "send"}
	if !validDeliveryPurpose(profileWelcome, "message", "capability_updated", "profile-1", "") {
		t.Fatal("profile welcome bound to a capability update was rejected")
	}
	if validDeliveryPurpose(profileWelcome, "message", "capability_updated", "other-profile", "") {
		t.Fatal("profile welcome bound to a different profile was accepted")
	}
}
