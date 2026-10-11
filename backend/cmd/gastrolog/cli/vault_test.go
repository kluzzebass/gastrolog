package cli

import (
	"context"
	"slices"
	"strings"
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// TestApplyVaultFlagsRetentionDisposition exercises the
// --retention-disposition validation switch directly (disposition accepts
// "transfer" alongside "delete"/"route"). No live client is touched — a nil
// *server.Client is safe here because none of these flag values are changed,
// so applyVaultFlags never reaches a client-resolving branch (cloud-service,
// rotation-policy, retention-policy, retention-transfer-target).
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

// seedRetentionPolicies stores named retention policies and returns their
// IDs in the same order.
func seedRetentionPolicies(t *testing.T, cfgStore *sysmem.Store, names ...string) []glid.GLID {
	t.Helper()
	ids := make([]glid.GLID, 0, len(names))
	for _, name := range names {
		maxAge := "720h"
		id := glid.New()
		if err := cfgStore.PutRetentionPolicy(context.Background(), system.RetentionPolicyConfig{ID: id, Name: name, MaxAge: &maxAge}); err != nil {
			t.Fatalf("seed retention policy %q: %v", name, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func runVaultCreate(t *testing.T, addr string, extra ...string) error {
	t.Helper()
	cmd := NewConfigCommand()
	AddClientFlags(cmd)
	cmd.SetArgs(append([]string{"vault", "create", "--addr", addr}, extra...))
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	return cmd.Execute()
}

func storedRetentionPolicyIDs(t *testing.T, cfgStore *sysmem.Store, vaultName string) []glid.GLID {
	t.Helper()
	vaults, err := cfgStore.ListVaults(context.Background())
	if err != nil {
		t.Fatalf("list vaults: %v", err)
	}
	for _, v := range vaults {
		if v.Name != vaultName {
			continue
		}
		ids := make([]glid.GLID, 0, len(v.RetentionRules))
		for _, r := range v.RetentionRules {
			ids = append(ids, r.RetentionPolicyID)
		}
		return ids
	}
	t.Fatalf("vault %q not stored", vaultName)
	return nil
}

// TestVaultCreateAttachesEveryRetentionPolicy drives the genuine
// client→RPC→store path: --retention-policy repeated, or comma-separated,
// attaches one rule per policy in the order given, and an empty value
// detaches them all on update.
func TestVaultCreateAttachesEveryRetentionPolicy(t *testing.T) {
	ts, cfgStore := newCloudServiceTestServer(t)
	ids := seedRetentionPolicies(t, cfgStore, "keep-30d", "cap-10gb")

	if err := runVaultCreate(t, ts.URL, "--name", "multi", "--type", "memory",
		"--retention-policy", "cap-10gb", "--retention-policy", "keep-30d"); err != nil {
		t.Fatalf("create with repeated flag: %v", err)
	}
	if got := storedRetentionPolicyIDs(t, cfgStore, "multi"); !slices.Equal(got, []glid.GLID{ids[1], ids[0]}) {
		t.Fatalf("repeated flag: rules = %v, want [%s %s] in flag order", got, ids[1], ids[0])
	}

	if err := runVaultCreate(t, ts.URL, "--name", "csv", "--type", "memory",
		"--retention-policy", "keep-30d,"+ids[1].String()); err != nil {
		t.Fatalf("create with comma-separated flag: %v", err)
	}
	if got := storedRetentionPolicyIDs(t, cfgStore, "csv"); !slices.Equal(got, ids) {
		t.Fatalf("comma-separated flag: rules = %v, want %v", got, ids)
	}

	if err := runVaultCreate(t, ts.URL, "--name", "multi", "--retention-policy", ""); err != nil {
		t.Fatalf("update with empty flag: %v", err)
	}
	if got := storedRetentionPolicyIDs(t, cfgStore, "multi"); len(got) != 0 {
		t.Fatalf("empty flag: rules = %v, want none", got)
	}

	err := runVaultCreate(t, ts.URL, "--name", "bad", "--type", "memory", "--retention-policy", "keep-30d,nope")
	if err == nil || !strings.Contains(err.Error(), `retention policy "nope" not found`) {
		t.Fatalf("unknown policy in the list: err = %v, want the policy named", err)
	}
}
