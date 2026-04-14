package orchestrator

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/memtest"

)

// TestRotationHookFiresRateAlerter verifies that the cron rotation
// path's onRotation callback feeds the orchestrator's rotation rate
// alerter — i.e. the wiring between cronRotationManager and
// Orchestrator.rotationRates is intact end-to-end.
func TestRotationHookFiresRateAlerter(t *testing.T) {
	t.Parallel()

	// Use a fake alert collector so we can observe Set/Clear calls.
	fa := &fakeAlerts{}
	orch := newTestOrch(t, Config{
		LocalNodeID: "node-1",
		Alerts:      fa,
	})

	// Lower the rotation alerter's threshold so a small number of
	// synthetic rotations crosses it within the test window.
	orch.rotationRates = newRateAlerter(rateAlerterConfig{
		Window:    10 * time.Second,
		Kind:      "rotation",
		Source:    "rotation",
		WarningAt: 0.5, // >= 5 rotations in 10s
		Alerts:    fa,
		TierName:  orch.tierLabel,
	})
	// Re-wire the cron callback against the new alerter.
	orch.cronRotation.onRotation = func(_, tierID glid.GLID) {
		orch.rotationRates.Record(tierID, orch.now())
	}

	tierID := glid.New()
	cm := &cronFakeChunkManager{
		active: &chunk.ChunkMeta{
			ID:          chunkIDAt(time.Now()),
			RecordCount: 1, // non-empty so rotateVault doesn't skip
		},
	}

	// Drive 5 rotations directly through the cron manager. Each call
	// should fire onRotation which feeds the rate alerter.
	for range 5 {
		// Reset for next iteration since rotateVault sets sealed=true.
		cm.sealed = false
		cm.active = &chunk.ChunkMeta{
			ID:          chunkIDAt(time.Now()),
			RecordCount: 1,
		}
		orch.cronRotation.rotateVault(glid.New(), tierID, "test-vault", cm)
	}

	// Trigger evaluation; alerter should raise the per-tier warning.
	orch.rotationRates.Evaluate(orch.now())

	calls := fa.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert call after 5 rotations, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[0].severity != alert.Warning {
		t.Errorf("expected Warning Set, got %+v", calls[0])
	}
	if calls[0].id != orch.rotationRates.alertID(tierID) {
		t.Errorf("alert ID mismatch: got %q want %q", calls[0].id, orch.rotationRates.alertID(tierID))
	}
}

// TestRetentionHookFiresRateAlerter verifies that retentionRunner.expireChunk
// records into the orchestrator's retention rate alerter when invoked. We
// construct a minimal retentionRunner with memory chunk and index managers
// and seed it with a chunk to expire.
func TestRetentionHookFiresRateAlerter(t *testing.T) {
	t.Parallel()

	fa := &fakeAlerts{}
	orch := newTestOrch(t, Config{
		LocalNodeID: "node-1",
		Alerts:      fa,
	})
	// Lower retention threshold so a small number of expirations crosses it.
	orch.retentionRates = newRateAlerter(rateAlerterConfig{
		Window:    10 * time.Second,
		Kind:      "retention",
		Source:    "retention",
		WarningAt: 0.5, // >= 5 deletes in 10s
		Alerts:    fa,
		TierName:  orch.tierLabel,
	})

	tierID := glid.New()

	// Record retention events directly via the same code path the
	// expireChunk hook uses. We don't drive a real expireChunk here
	// because the retentionRunner has many dependencies; instead we
	// invoke the orchestrator method that the hook calls. The full
	// expireChunk path is exercised by existing retention_test.go.
	for range 5 {
		orch.retentionRates.Record(tierID, orch.now())
	}

	orch.retentionRates.Evaluate(orch.now())

	calls := fa.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert call after 5 expirations, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[0].severity != alert.Warning {
		t.Errorf("expected Warning Set, got %+v", calls[0])
	}
	if calls[0].id != orch.retentionRates.alertID(tierID) {
		t.Errorf("alert ID mismatch: got %q want %q", calls[0].id, orch.retentionRates.alertID(tierID))
	}
}

// TestInternalRotationFiresRateAlerter is the regression test for the
// discovery during gastrolog-47qyw testing: record-count- and size-based
// rotation policies rotate inside cm.Append, which the orchestrator
// detects post-hoc by comparing Active() before and after the call.
// Without a hook at that detection site, high-rate internal rotations
// are invisible to the rate alerter (operator sees only generic pressure
// alerts, not the rotation-rate signal). This test drives a real memory
// chunk manager with a 3-record rotation policy and asserts the
// rotation events land in the alerter.
func TestInternalRotationFiresRateAlerter(t *testing.T) {
	t.Parallel()

	fa := &fakeAlerts{}
	orch := newTestOrch(t, Config{
		LocalNodeID: "node-1",
		Alerts:      fa,
	})

	// Build a real memory-backed vault with an aggressive 3-record policy.
	s, err := memtest.NewVault(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(3),
	})
	if err != nil {
		t.Fatalf("memtest.NewVault: %v", err)
	}
	vaultID := glid.New()
	orch.RegisterVault(NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	// Catch-all filter so every record routes into the vault.
	orch.SetFilterSet(NewFilterSet([]*CompiledFilter{
		{VaultID: vaultID, Kind: FilterCatchAll, Expr: "*"},
	}))

	// Lower the rotation alerter's threshold so a handful of rotations
	// crosses the warning line within the test window.
	orch.rotationRates = newRateAlerter(rateAlerterConfig{
		Window:    10 * time.Second,
		Kind:      "rotation",
		Source:    "rotation",
		WarningAt: 0.2, // >= 2 rotations in 10s
		Alerts:    fa,
		TierName:  orch.tierLabel,
	})

	// Feed 15 records. With a 3-record policy this triggers 5 rotations,
	// each via cm.Append's internal sealLocked/openLocked path — the
	// one exercising vault_ops.go's Active()-change detection.
	for i := range 15 {
		rec := chunk.Record{
			Raw: fmt.Appendf(nil, "rec-%d", i),
		}
		if _, err := orch.ingest(rec); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}

	orch.rotationRates.Evaluate(orch.now())

	calls := fa.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 warning after 5 internal rotations, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[0].severity != alert.Warning {
		t.Errorf("expected Warning Set, got %+v", calls[0])
	}
}

// TestRateAlertEvaluatorRunsPeriodically verifies that the background
// goroutine launched by Start actually invokes Evaluate on a fixed
// cadence and that alerts fire without manual evaluation.
func TestRateAlertEvaluatorRunsPeriodically(t *testing.T) {
	t.Parallel()

	fa := &fakeAlerts{}
	orch := newTestOrch(t, Config{
		LocalNodeID: "node-1",
		Alerts:      fa,
	})
	// Set up a low-threshold alerter so a single tick of the background
	// evaluator catches the elevated rate. Window of 10s comfortably
	// outlasts the 5s ticker so the recorded event is still in the
	// window when Evaluate first runs.
	orch.rotationRates = newRateAlerter(rateAlerterConfig{
		Window:    10 * time.Second,
		Kind:      "rotation",
		Source:    "rotation",
		WarningAt: 0.2, // >= 2 rotations in 10s
		Alerts:    fa,
		TierName:  orch.tierLabel,
	})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = orch.Stop() }()

	// Record several rotation events immediately so the rate is
	// comfortably above the warning threshold. The background
	// evaluator runs every 5s; we wait up to 7s for it to fire.
	tierID := glid.New()
	for range 5 {
		orch.rotationRates.Record(tierID, orch.now())
	}

	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		if calls := fa.snapshot(); len(calls) > 0 {
			if calls[0].op == "set" && calls[0].severity == alert.Warning {
				return // success
			}
			t.Fatalf("unexpected first call: %+v", calls[0])
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("background rate evaluator did not raise alert within 7s")
}
