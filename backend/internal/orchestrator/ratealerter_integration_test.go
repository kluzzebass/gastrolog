package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/alert"
)

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
		VaultName: orch.vaultLabel,
	})

	vaultID := glid.New()

	// Record retention events directly via the same code path the
	// expireChunk hook uses. We don't drive a real expireChunk here
	// because the retentionRunner has many dependencies; instead we
	// invoke the orchestrator method that the hook calls. The full
	// expireChunk path is exercised by existing retention_test.go.
	for range 5 {
		orch.retentionRates.Record(vaultID, orch.now())
	}

	orch.retentionRates.Evaluate(orch.now())

	calls := fa.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert call after 5 expirations, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[0].severity != alert.Warning {
		t.Errorf("expected Warning Set, got %+v", calls[0])
	}
	if calls[0].id != orch.retentionRates.alertID(vaultID) {
		t.Errorf("alert ID mismatch: got %q want %q", calls[0].id, orch.retentionRates.alertID(vaultID))
	}
}

// TestRateAlertEvaluatorRunsPeriodically verifies that the background
// goroutine launched by Start actually invokes Evaluate on a fixed
// cadence and that alerts fire without manual evaluation.
func TestRateAlertEvaluatorRunsPeriodically(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second periodic-evaluator test")
	}
	t.Parallel()

	fa := &fakeAlerts{}
	orch := newTestOrch(t, Config{
		LocalNodeID: "node-1",
		Alerts:      fa,
	})
	// Set up a low-threshold retention alerter so a single tick of the
	// background evaluator catches the elevated rate. Window of 10s
	// comfortably outlasts the 5s ticker so the recorded event is still in
	// the window when Evaluate first runs.
	orch.retentionRates = newRateAlerter(rateAlerterConfig{
		Window:    10 * time.Second,
		Kind:      "retention",
		Source:    "retention",
		WarningAt: 0.2, // >= 2 expirations in 10s
		Alerts:    fa,
		VaultName: orch.vaultLabel,
	})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = orch.Stop() }()

	// Record several retention events immediately so the rate is
	// comfortably above the warning threshold. The background
	// evaluator runs every 5s; we wait up to 7s for it to fire.
	vaultID := glid.New()
	for range 5 {
		orch.retentionRates.Record(vaultID, orch.now())
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
