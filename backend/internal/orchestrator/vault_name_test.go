package orchestrator

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
)

// staticSystemLoader serves one fixed config, which is all VaultAlarmLabel
// needs: the vault name's owner is the config, not the vault registry.
type staticSystemLoader struct{ sys *system.System }

func (l *staticSystemLoader) Load(context.Context) (*system.System, error) { return l.sys, nil }

func orchWithVaults(t *testing.T, vaults ...system.VaultConfig) *Orchestrator {
	t.Helper()
	sys := &system.System{}
	sys.Config.Vaults = vaults
	orch, err := New(Config{
		SegmentsDir:  t.TempDir(),
		SystemLoader: &staticSystemLoader{sys: sys},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return orch
}

// Alarm text names a vault the way the operator knows it. VaultAlarmLabel is
// the lookup that makes that possible from raise sites holding only an ID, so
// its miss behaviour is load-bearing: an unresolvable vault degrades to the ID
// rather than being announced as an empty pair of quotes.
func TestVaultAlarmLabelNamesTheVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := orchWithVaults(t, system.VaultConfig{ID: vaultID, Name: "app-logs"})

	if got := orch.VaultAlarmLabel(vaultID); got != `"app-logs"` {
		t.Errorf("VaultAlarmLabel = %s, want %s", got, `"app-logs"`)
	}

	unknown := glid.New()
	if got := orch.VaultAlarmLabel(unknown); got != unknown.String() {
		t.Errorf("VaultAlarmLabel for an unconfigured vault = %q, want the bare ID %q", got, unknown.String())
	}
}

// The name resolves from the config on every call rather than being captured at
// wiring time, so an alarm raised after a rename announces the new name.
func TestVaultAlarmLabelFollowsARename(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sys := &system.System{}
	sys.Config.Vaults = []system.VaultConfig{{ID: vaultID, Name: "app-logs"}}
	orch, err := New(Config{SegmentsDir: t.TempDir(), SystemLoader: &staticSystemLoader{sys: sys}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got := orch.VaultAlarmLabel(vaultID); got != `"app-logs"` {
		t.Fatalf("VaultAlarmLabel = %s, want %s", got, `"app-logs"`)
	}
	sys.Config.Vaults[0].Name = "application-logs"
	if got := orch.VaultAlarmLabel(vaultID); got != `"application-logs"` {
		t.Errorf("VaultAlarmLabel after rename = %s, want %s", got, `"application-logs"`)
	}
}

// buildPipelineVaultSpec is what hands segmentation and chunking their name
// resolver; without it their fallback becomes the only path and every alarm
// from those packages goes back to printing a bare GLID.
func TestPipelineVaultSpecCarriesTheNameResolver(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := orchWithVaults(t, system.VaultConfig{ID: vaultID, Name: "app-logs"})

	spec, err := orch.buildPipelineVaultSpec(vaultID, false, nil, nil, nil, false, chunking.ManifestRotationPolicy{})
	if err != nil {
		t.Fatalf("buildPipelineVaultSpec: %v", err)
	}
	if spec.VaultName == nil {
		t.Fatal("spec.VaultName is nil — segmentation and chunking alarms will fall back to the vault ID")
	}
	if got := spec.VaultName(); got != "app-logs" {
		t.Errorf("spec.VaultName() = %q, want %q", got, "app-logs")
	}
}
