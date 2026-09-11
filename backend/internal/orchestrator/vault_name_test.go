package orchestrator

import (
	"context"
	"sync"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
)

// staticSystemLoader serves one config, which is all VaultAlarmLabel needs:
// the vault name's owner is the config, not the vault registry.
//
// Changing it means swapping the whole config, never editing the one already
// served. The orchestrator's scheduler loads config from its own goroutines,
// so an in-place edit races them and the detector then fails whichever tests
// happened to be running.
type staticSystemLoader struct {
	mu  sync.RWMutex
	sys *system.System
}

func newStaticSystemLoader(vaults ...system.VaultConfig) *staticSystemLoader {
	l := &staticSystemLoader{}
	l.setVaults(vaults...)
	return l
}

func (l *staticSystemLoader) Load(context.Context) (*system.System, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.sys, nil
}

func (l *staticSystemLoader) setVaults(vaults ...system.VaultConfig) {
	sys := &system.System{}
	sys.Config.Vaults = vaults
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sys = sys
}

func orchWithVaults(t *testing.T, vaults ...system.VaultConfig) *Orchestrator {
	t.Helper()
	orch, err := New(Config{
		SegmentsDir:  t.TempDir(),
		SystemLoader: newStaticSystemLoader(vaults...),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Scheduler().Stop() })
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
	loader := newStaticSystemLoader(system.VaultConfig{ID: vaultID, Name: "app-logs"})
	orch, err := New(Config{SegmentsDir: t.TempDir(), SystemLoader: loader})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Scheduler().Stop() })

	if got := orch.VaultAlarmLabel(vaultID); got != `"app-logs"` {
		t.Fatalf("VaultAlarmLabel = %s, want %s", got, `"app-logs"`)
	}
	loader.setVaults(system.VaultConfig{ID: vaultID, Name: "application-logs"})
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
