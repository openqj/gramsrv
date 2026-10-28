package help

import (
	"context"
	"encoding/json"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestAppConfigPublishesAgentBotOnlyWhenConfigured(t *testing.T) {
	serviceOff := NewService(nil, nil)
	offCfg, _, err := serviceOff.GetAppConfig(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("GetAppConfig: %v", err)
	}
	var offValues map[string]json.RawMessage
	if err := json.Unmarshal(offCfg.JSON, &offValues); err != nil {
		t.Fatalf("decode disabled app config: %v", err)
	}
	if _, exists := offValues["agent_bot_username"]; exists {
		t.Fatal("agent_bot_username was published while Gateway was disabled")
	}
	if offCfg.Hash != defaultAppConfigHash {
		t.Fatalf("disabled app config hash = %d, want %d", offCfg.Hash, defaultAppConfigHash)
	}

	serviceOn := NewService(nil, nil, WithAgentBotUsername("@ChatBot"))
	onCfg, notModified, err := serviceOn.GetAppConfig(context.Background(), 0, offCfg.Hash)
	if err != nil || notModified {
		t.Fatalf("enabled GetAppConfig(off hash) = notModified %v, err %v", notModified, err)
	}
	var onValues map[string]json.RawMessage
	if err := json.Unmarshal(onCfg.JSON, &onValues); err != nil {
		t.Fatalf("decode enabled app config: %v", err)
	}
	var username string
	if err := json.Unmarshal(onValues["agent_bot_username"], &username); err != nil || username != domain.ChatBotUser().Username {
		t.Fatalf("agent_bot_username = %q, err=%v", username, err)
	}
	if onCfg.Hash == offCfg.Hash {
		t.Fatal("Gateway enablement did not change the app config hash")
	}

	stored := memory.NewHelpStore()
	if err := stored.UpsertAppConfig(context.Background(), domain.AppConfig{
		Client: tdesktopClient, Hash: defaultAppConfigHash + 100,
		JSON: defaultAppConfigJSONWithBots("", domain.PremiumBotUser().Username, domain.ChatBotUser().Username, false),
	}); err != nil {
		t.Fatal(err)
	}
	filtered, _, err := NewService(stored, nil).GetAppConfig(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("GetAppConfig(stored config): %v", err)
	}
	var filteredValues map[string]json.RawMessage
	if err := json.Unmarshal(filtered.JSON, &filteredValues); err != nil {
		t.Fatalf("decode filtered app config: %v", err)
	}
	if _, exists := filteredValues["agent_bot_username"]; exists {
		t.Fatal("stored app config exposed Agent bot while Gateway was disabled")
	}
}
