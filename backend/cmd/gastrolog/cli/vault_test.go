package cli

import (
	"context"
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// TestApplyVaultFlagsRetentionDisposition exercises the
// --retention-disposition validation switch directly (gastrolog-2l918:
// disposition gains "transfer" alongside "delete"/"route"). No live client
// is touched — a nil *server.Client is safe here because none of these
// flag values are changed, so applyVaultFlags never reaches a
// client-resolving branch (cloud-service, rotation-policy, retention-policy,
// retention-transfer-target).
func TestApplyVaultFlagsRetentionDisposition(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"delete", "delete", false},
		{"route", "route", false},
		{"transfer", "transfer", false},
		{"bogus value rejected", "bogus", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newVaultCreateCmd()
			if err := cmd.Flags().Set("retention-disposition", tt.value); err != nil {
				t.Fatalf("set flag: %v", err)
			}
			cfg := &v1.VaultConfig{}
			err := applyVaultFlags(context.Background(), cmd, nil, cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("applyVaultFlags(%q): expected error, got nil", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyVaultFlags(%q): unexpected error: %v", tt.value, err)
			}
			if cfg.RetentionDisposition != tt.value {
				t.Errorf("RetentionDisposition = %q, want %q", cfg.RetentionDisposition, tt.value)
			}
		})
	}
}

// TestResolveVaultRetentionTransferTargetEmptyClears proves an explicit
// empty --retention-transfer-target clears any existing target rather than
// requiring a resolver round-trip — same pattern as
// resolveVaultCloudService/resolveVaultRotationPolicy for their "" case.
func TestResolveVaultRetentionTransferTargetEmptyClears(t *testing.T) {
	cmd := newVaultCreateCmd()
	if err := cmd.Flags().Set("retention-transfer-target", ""); err != nil {
		t.Fatalf("set flag: %v", err)
	}
	cfg := &v1.VaultConfig{RetentionTransferTargetVaultId: []byte{1, 2, 3}}
	if err := resolveVaultRetentionTransferTarget(context.Background(), cmd, nil, cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RetentionTransferTargetVaultId != nil {
		t.Errorf("RetentionTransferTargetVaultId = %v, want nil (cleared)", cfg.RetentionTransferTargetVaultId)
	}
}
