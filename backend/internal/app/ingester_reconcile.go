package app

import (
	"context"
	"log/slog"
	"slices"
	"strings"
)

const (
	ingesterReconcileJobName = "ingester-reconcile"
	// Offset from the placement reconcile's */15 so the two sweeps don't
	// stack on the same tick.
	ingesterReconcileSchedule = "7,22,37,52 * * * * *"
)

// startIngesterReconcileSweep registers the periodic ingester convergence
// sweep — the safety net behind the event-driven reconciles. Dispatch is
// otherwise one-shot per FSM notification with silent early returns (a store
// list error, or a node not yet ready at boot dispatch, simply skips): a node
// that misses its trigger runs no ingesters until the next config change. A
// full-cluster restart left one node with zero ingesters for 40+ minutes
// while its peers ingested. reconcileIngesters is idempotent and never flaps
// running ingesters, so the sweep is safe to run unconditionally; it also
// logs divergence — desired ingesters not actually running — so a
// convergence failure is never silent.
//
// Divergence is a LOG, not an alarm (operator razor): the sweep itself
// re-dispatches every tick and the run loop retries failed ingesters with
// backoff — the system is already doing everything an operator could ask.
// The condition is normal for a tick or two at boot, and the failure detail
// an operator would act on (build/start errors) is already in the log the
// self-ingester captures.
func startIngesterReconcileSweep(ctx context.Context, scheduler scheduledJobRegistry, d *configDispatcher, logger *slog.Logger) error {
	task := func() {
		// Route through settle so this existing safety-net sweep doubles as
		// the clear/retry point for a standing ingester reconcile obligation:
		// a successful pass clears it, a still-failing pass keeps it standing.
		// This reuses an existing trigger — it does not add a new sweep — and
		// is distinct from the divergence LOG below (desired ingester not
		// running), which stays a log per the operator razor.
		d.settle(entIngester, "", "reconcile-ingesters", d.reconcileIngesters(ctx))
		d.reportIngesterDivergence(ctx, logger)
	}
	if err := scheduler.AddJob(ingesterReconcileJobName, ingesterReconcileSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(ingesterReconcileJobName,
		"Ingester convergence sweep — periodic safety net. Recomputes the ingesters this node should run from config and drives the orchestrator toward it (idempotent; running ingesters never flap), then logs any desired ingester that is still not running. Event-driven reconciles (config puts, singleton assignment) remain the fast path; this tick heals missed triggers such as a node that was not ready at boot dispatch.")
	return nil
}

// reportIngesterDivergence logs the set of ingesters this node should be
// running but is not — once per state CHANGE, never per tick: the sweep runs
// every 15 seconds and a wedged ingester must not fill the log with the same
// line (the same once-on-cross, once-on-resolve shape the channel-pressure
// watchdog uses). Runs after the sweep's reconcile, so anything listed failed
// to start (or to build) rather than merely awaiting dispatch.
func (d *configDispatcher) reportIngesterDivergence(ctx context.Context, logger *slog.Logger) {
	cfgs, err := d.cfgStore.ListIngesters(ctx)
	if err != nil {
		// The reconcile pass already logged; keep the last-reported state
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
	slices.Sort(missing)
	state := strings.Join(missing, ", ")
	if state == d.lastIngesterDivergence {
		return
	}
	prev := d.lastIngesterDivergence
	d.lastIngesterDivergence = state
	if logger == nil {
		return
	}
	if state == "" {
		logger.Info("ingester convergence restored — all desired ingesters running",
			"previously_missing", prev)
		return
	}
	logger.Warn("desired ingester(s) not running — sweep will keep re-dispatching, runs retry with backoff",
		"missing", state, "count", len(missing))
}
