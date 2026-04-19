package system

import "context"

// Dispatcher notification keys for server settings persisted via
// PutSettingCommand (Raft). Empty notify_key in the command defaults to
// NotifyKeyServerSettingsRaftLegacy for log compatibility.
const (
	NotifyKeyServerSettingsRaftLegacy = "server"
	NotifyKeyServiceSettings          = "service_settings"
	NotifyKeyLookupSettings           = "lookup_settings"
	NotifyKeyMaxMindSettings          = "maxmind_settings"
)

type saveServerSettingsNotifyKey struct{}

// WithSaveServerSettingsNotifyKey attaches a notify key read by raft-backed
// SaveServerSettings when building the PutSettingCommand.
func WithSaveServerSettingsNotifyKey(ctx context.Context, notifyKey string) context.Context {
	return context.WithValue(ctx, saveServerSettingsNotifyKey{}, notifyKey)
}

// SaveServerSettingsNotifyKey returns the notify key from ctx, or "" if unset.
func SaveServerSettingsNotifyKey(ctx context.Context) string {
	v, _ := ctx.Value(saveServerSettingsNotifyKey{}).(string)
	return v
}
