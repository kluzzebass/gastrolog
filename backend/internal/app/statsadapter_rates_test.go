package app

// End-to-end reproduction rig for gastrolog-4eh5ns "rates show 0/s": real
// orchestrator + pipeline routing + the REAL orchStatsAdapter + real
// StatsCollector ticks. If this passes, the backend produces nonzero rates
// from live traffic and the fault is in serving or deployment; if it fails,
// the window/adapter chain is broken.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

func TestStatsCollectorRouteRatesEndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: cfgStore,
		SegmentsDir:  filepath.Join(t.TempDir(), "segments"),
	})
	if err != nil {
		t.Fatal(err)
	}

	vaultID := glid.New()
	routeID := glid.New()
	if err := cfgStore.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "rate-vault", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := cfgStore.PutRoute(ctx, system.RouteConfig{
		ID:           routeID,
		Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
		Destinations: []glid.GLID{vaultID},
		Enabled:      true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatal(err)
	}
	if err := orch.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = orch.Stop() })

	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{
		Stats:      &orchStatsAdapter{orch: orch},
		NodeID:     "node-test",
		NodeNameFn: func() string { return "node-test" },
	})

	// Tick 1: initializes the route window against the current counters.
	t0 := time.Now()
	_ = collector.CollectLocalTick(t0)

	// Drive records through the real routing stage.
	const n = 100
	for range n {
		if err := orch.SubmitRetentionRecord(ctx, vaultID, chunk.Record{Raw: []byte("rate probe")}, ""); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for orch.GetRouteStats().Routed < n {
		if time.Now().After(deadline) {
			t.Fatalf("routing counters never reached %d (routed=%d)", n, orch.GetRouteStats().Routed)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Tick 2, nominally 5s later: rate must be n/5 records per second.
	tick := collector.CollectLocalTick(t0.Add(5 * time.Second))
	if tick.RouteRouted.GetInstantPerSec() <= 0 {
		t.Fatalf("tick route routed = %v after %d records, want > 0", tick.RouteRouted.GetInstantPerSec(), n)
	}

	// The snapshot path (what GetRouteStats reads via LocalStats) must report
	// the last stepped rates — instant AND trailing averages.
	snap := collector.CollectLocalSnapshot()
	if snap.RouteRouted.GetInstantPerSec() <= 0 {
		t.Fatalf("snapshot route routed = %v, want > 0 (last stepped rate)", snap.RouteRouted.GetInstantPerSec())
	}
	if snap.RouteMatched.GetInstantPerSec() <= 0 {
		t.Fatalf("snapshot route routed = %v, want > 0", snap.RouteMatched.GetInstantPerSec())
	}
	if snap.RouteRouted.GetAvg_1MPerSec() <= 0 || snap.RouteRouted.GetAvg_15MPerSec() <= 0 {
		t.Fatalf("snapshot EWMAs = %v/%v, want > 0",
			snap.RouteRouted.GetAvg_1MPerSec(), snap.RouteRouted.GetAvg_15MPerSec())
	}
}

// TestStatsAdapterVaultSnapshotsCarryNames pins the vault-name passthrough:
// the inspector's per-vault throughput rows label vaults by name and fall
// back to a GLID prefix when the broadcast carries an empty name — which is
// exactly what shipped when orchStatsAdapter dropped the field (user-visible
// as "06FMA5GB"-style labels in the System panel).
func TestStatsAdapterVaultSnapshotsCarryNames(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: cfgStore,
		SegmentsDir:  filepath.Join(t.TempDir(), "segments"),
	})
	if err != nil {
		t.Fatal(err)
	}
	vaultID := glid.New()
	if err := cfgStore.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "named-vault", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVault(vaultID, nil))

	adapter := &orchStatsAdapter{orch: orch}
	snaps := adapter.VaultSnapshots()
	if len(snaps) == 0 {
		t.Fatal("expected at least one vault snapshot")
	}
	for _, s := range snaps {
		if s.ID == vaultID && s.Name != "named-vault" {
			t.Fatalf("snapshot name = %q, want %q — stats broadcast must carry vault names", s.Name, "named-vault")
		}
		if s.ID == vaultID {
			return
		}
	}
	t.Fatalf("vault %s not found in snapshots", vaultID)
}
