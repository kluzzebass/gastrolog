package app

import (
	"context"
	"fmt"
	"strings"

	"gastrolog/internal/alert"
	"gastrolog/internal/orchestrator"
)

const (
	ingesterReconcileJobName = "ingester-reconcile"
	// Offset from the placement reconcile's */15 so the two sweeps don't
	// stack on the same tick.
	ingesterReconcileSchedule = "7,22,37,52 * * * * *"

	ingesterNotRunningAlertID = "ingester-not-running"
)

// startIngesterReconcileSweep registers the periodic ingester convergence
// sweep — the safety net behind the event-driven reconciles. Dispatch is
// otherwise one-shot per FSM notification with silent early returns (a store
// list error, or a node not yet ready at boot dispatch, simply skips): a node
// that misses its trigger runs no ingesters until the next config change. A
// full-cluster restart left one node with zero ingesters for 40+ minutes
// while its peers ingested (gastrolog-3mnjlo). reconcileIngesters is
// idempotent and never flaps running ingesters, so the sweep is safe to run
// unconditionally; it also raises the divergence alert — desired ingesters
// not actually running — so a convergence failure is never silent.
func startIngesterReconcileSweep(ctx context.Context, scheduler scheduledJobRegistry, d *configDispatcher, alerts orchestrator.AlertCollector) error {
	task := func() {
		d.reconcileIngesters(ctx)
		d.reportIngesterDivergence(ctx, alerts)
	}
	if err := scheduler.AddJob(ingesterReconcileJobName, ingesterReconcileSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(ingesterReconcileJobName,
		"Ingester convergence sweep — periodic safety net. Recomputes the ingesters this node should run from config and drives the orchestrator toward it (idempotent; running ingesters never flap), then alarms on any desired ingester that is still not running. Event-driven reconciles (config puts, singleton assignment) remain the fast path; this tick heals missed triggers such as a node that was not ready at boot dispatch.")
	return nil
}

// reportIngesterDivergence raises one alert naming every ingester this node
// should be running but is not, and clears it once converged. Runs after the
// sweep's reconcile, so anything listed failed to start (or to build) rather
// than merely awaiting dispatch.
func (d *configDispatcher) reportIngesterDivergence(ctx context.Context, alerts orchestrator.AlertCollector) {
	if alerts == nil {
		return
	}
	cfgs, err := d.cfgStore.ListIngesters(ctx)
	if err != nil {
		// The reconcile pass already logged; keep the standing alert state
		// rather than flapping on a transient store error.
		return
	}
	var missing []string
	for _, cfg := range cfgs {
		if !cfg.Enabled {
			continue
		}
		if _, ok := d.factories.IngesterTypes[cfg.Type]; !ok {
			continue // unknown type is its own log noise; not a dispatch gap
		}
		isSingleton := d.factories.IngesterTypes[cfg.Type].SingletonSupported && cfg.Singleton
		if !d.shouldRunIngester(ctx, cfg, isSingleton) {
			continue
		}
		if !d.orch.IsIngesterRunning(cfg.ID) {
			missing = append(missing, cfg.Name)
		}
	}
	if len(missing) == 0 {
		alerts.Clear(ingesterNotRunningAlertID)
		return
	}
	alerts.Set(ingesterNotRunningAlertID, alert.Error, "ingestion",
		fmt.Sprintf("This node should be running %d ingester(s) that are not running: %s. Ingestion capacity is reduced until they start; check the log for build/start errors.",
			len(missing), strings.Join(missing, ", ")))
}
