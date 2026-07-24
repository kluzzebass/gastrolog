package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator/pipeline"
)

// Start launches the ingest pipeline and the orchestrator's auxiliary
// goroutines. New live data flows ingest→digest→route→segment, acked only after
// a durable segment write (ack-after-fsync). Start returns immediately; use
// Stop() to shut down.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	// Create cancellable context for the pipeline and aux goroutines.
	ctx, cancel := context.WithCancel(ctx)
	o.cancel = cancel

	// Log startup info.
	o.logger.Info("starting orchestrator",
		"vaults", len(o.vaults),
		"ingesters", len(o.ingesters))

	if !o.pipeline.RoutingActive() && len(o.vaults) > 0 {
		o.logger.Warn("no routes configured, ingested records will be dropped")
	}

	// Start shared scheduler (cron rotation, retention, and future scheduled tasks).
	o.scheduler.Start()

	// Pipeline pressure gate. PressureAware ingesters consult it to throttle
	// when the pipeline backs up; the supervisor's ingestion manager injects it
	// into each ingester. Bounded inter-phase queues still block on send and
	// remain the primary backpressure mechanism for the durable write path.
	//
	// Pressure raises no alarm: if ingestion is throttled, the matter is
	// already handled — the throttle IS the response. An alarm announces a
	// condition waiting on a human, and this one is not waiting on anyone.
	// The cases where a backlog goes on to threaten something are carried by
	// the pipeline-backlog and disk-space alarms, which do have an operator
	// action. The level ships in NodeStats as ingest_pressure_level for the
	// health surfaces (gastrolog-3phtqv).
	//
	// It is deliberately not logged either: the self-ingester captures slog
	// records, so logging throttle transitions would feed the very pressure
	// being reported.
	gate := o.pipelineGate
	ingestProbe := func() (int, int) {
		return o.pipeline.IngestQueueDepth(), o.pipeline.IngestQueueCapacity()
	}
	gate.AddProbe("ingest-digest", ingestProbe)

	// Prime the disk guard BEFORE admission opens: a node restarting into an
	// already-full volume must reject from the first record, not after the
	// first 15s scheduler tick (gastrolog-67gvjo — the boot blind window that
	// killed the second wave of nodes).
	o.primeDiskGuard()

	// Push the bootstrap-registered ingester set into the supervisor, then start
	// the pipeline (the ingestion manager launches the ingesters).
	if err := o.pushIngestersToSupervisorLocked(); err != nil {
		o.logger.Error("reconcile ingesters at startup", "error", err)
	}
	if err := o.pipeline.Start(ctx); err != nil {
		o.running.Store(false)
		cancel()
		o.cancel = nil
		return fmt.Errorf("start pipeline: %w", err)
	}

	// Channel pressure watchdog — logs at 90%, separate from the hysteresis
	// gate used for throttling (logs once on cross, once on resolve).
	cw := chanwatch.New(o.logger, time.Second)
	cw.Watch("ingest-digest", ingestProbe, 0.9)
	o.auxWg.Go(func() { cw.Run(ctx) })

	// Start the pressure gate after everything else is wired.
	o.auxWg.Go(func() { gate.Run(ctx, 200*time.Millisecond) })

	// Periodic retention-rate + cloud-health evaluation on the shared
	// scheduler (gastrolog-47qyw, gastrolog-576bm). Replaces the raw 5s
	// time.Ticker (runRateAlertEvaluator) that was invisible to the scheduler
	// and inspector. Rate-threshold evaluation and cloud-store reachability
	// polling are legitimate periodic policy work; the cloud-upload backfill
	// this loop used to drive every tick is now edge-triggered (seal effect,
	// leadership gain, snapshot restore) — see evaluateVaultCloudHealth.
	// Registered here rather than in New() so it runs only while the
	// orchestrator is started, matching the retired ticker's lifecycle. A
	// registration failure degrades alerting but must not abort startup.
	if err := o.startCloudHealthAndRateAlerts(); err != nil {
		o.logger.Error("register cloud-health/rate-alert job", "error", err)
	}

	// Active-chunk progress throttle (gastrolog-4y03v). Append paths
	// signal progressTrigger per record; this goroutine fans out to
	// chunkSignal at most once per window with leading-edge fire on
	// the first signal after quiet, plus a trailing-edge fire if the
	// burst continued through the window. Idle cluster: zero work.
	o.auxWg.Go(func() { o.runProgressNotifier(ctx, time.Second) })

	// Active-chunk progress emitter (gastrolog-3pf9w). Polls every
	// leader vault's active chunk record count once per second; emits a
	// typed PROGRESS event on the chunk bus when the count has
	// advanced since the last tick. Bounded to one event per active
	// chunk per second regardless of append rate. WatchChunks
	// subscribers patch their cache directly from this event instead
	// of refetching the world.
	o.auxWg.Go(func() { o.runChunkProgressEmitter(ctx, time.Second) })

	// Job-event slog bridge (gastrolog-5mcqm follow-up). Subscribes to
	// the scheduler's event broker and emits a structured slog entry
	// per transition. Captured by the self ingester so job lifecycle
	// is searchable like any other log.
	o.auxWg.Go(func() { o.runJobEventLogBridge(ctx) })

	// Readiness refresher (gastrolog-5n6xz). Publishes the cached
	// LocalVaultsReplicationReady value the /readyz handler reads, so
	// kubelet's probe stays responsive when o.mu is contended by a
	// vault-ctl AddVoter burst on K8s scale-out.
	o.auxWg.Go(func() { o.runReadinessRefresher(ctx, readinessRefreshInterval) })

	// Lock-leak reporter (gastrolog-1ug3rq): names orphaned o.mu holds and
	// stuck write waiters with their acquisition stacks. Reads only tracker
	// state — survives an o.mu wedge by construction.
	o.auxWg.Go(func() { o.runLockLeakReporter(ctx) })

	// Seed the cache synchronously while o.mu is held so /readyz is correct
	// immediately after Start() — the async refresher's first tick can lag
	// by up to readinessRefreshInterval and would otherwise leave the
	// constructor's optimistic true seed visible (gastrolog-5n6xz).
	o.cachedReplicationReady.Store(o.liveReplicationReadyLocked())

	return nil
}

// rateAlertAndCloudHealthTick is the periodic scheduler job body that evaluates
// the retention rate alerter (gastrolog-47qyw) and cloud health (raise/clear the
// cloud-store alert, plus edge-triggered upload catch-up — see
// evaluateVaultCloudHealth). It replaces the raw 5s time.Ticker that was
// invisible to the scheduler/inspector and doubled as the cloud-upload
// compensator (gastrolog-576bm); the rate-alert evaluation is legitimate
// periodic policy work, so it stays periodic — only the backfill mechanism was
// demoted off the tick.
func (o *Orchestrator) rateAlertAndCloudHealthTick() {
	o.retentionRates.Evaluate(o.now())
	o.evaluateCloudHealth()
}

// Stop stops the ingest pipeline and the orchestrator's auxiliary
// goroutines, then waits for everything to finish.
//
// Ordered shutdown:
//  0. BeginShutdown on the shared phase (if wired) → fast-path skip in
//     sealed-chunk replication so the drain pipeline doesn't spam peers
//     that are going down alongside us.
//  1. Stop the pipeline supervisor — ingestion stops first (closing the ingest
//     queue), cascading through digest→route→segment as each queue closes,
//     then the remaining managers are cancelled. Ingesters' alive state is
//     cleared by the adapter as they exit.
//  2. Cancel the orchestrator context and wait for aux goroutines.
func (o *Orchestrator) Stop() error {
	if !o.running.CompareAndSwap(true, false) {
		return ErrNotRunning
	}
	o.mu.Lock()
	cancel := o.cancel
	o.mu.Unlock()

	// Stage 0: flip the shutdown phase BEFORE any drain work so that
	// sealed-chunk replication skips its remote calls while we drain.
	// Idempotent if the top-level shutdown already flipped it; safe to call
	// with a nil phase (single-node tests). See gastrolog-1e5ke.
	if o.phase != nil {
		o.phase.BeginShutdown("orchestrator: stopping pipeline")
	}

	// Stage 1: stop the pipeline and wait for all phase managers to exit.
	if err := o.pipeline.Stop(); err != nil && !errors.Is(err, pipeline.ErrNotRunning) {
		o.logger.Error("stop pipeline", "error", err)
	}

	// Stage 2: cancel the orchestrator context and wait for aux goroutines.
	if cancel != nil {
		cancel()
	}
	o.auxWg.Wait()

	// Stop shared scheduler — waits for running jobs (index builds,
	// cron rotation, retention) to finish.
	_ = o.scheduler.Stop()

	// Stop all per-vault leader loops (after the scheduler so reconcile
	// passes don't fight retention deletes during shutdown).
	o.vaultCtlLeaders.StopAll()

	// Stage 3: close every vault instance — releases chunk-manager
	// directory flocks, mmaps, and file handles. Stop used to leave
	// managers open until process exit, which production never noticed
	// (the OS releases flocks with the process) but which made any
	// in-process restart fail vault init with ErrDirectoryLocked — and a
	// failed init is skipped, not retried, so the vault stayed dead.
	o.mu.Lock()
	vaults := make([]*Vault, 0, len(o.vaults))
	for _, v := range o.vaults {
		vaults = append(vaults, v)
	}
	o.mu.Unlock()
	for _, v := range vaults {
		if err := v.Close(); err != nil {
			o.logger.Warn("close vault instance at stop", "error", err)
		}
	}

	o.mu.Lock()
	o.cancel = nil
	o.mu.Unlock()

	return nil
}

// Close releases scheduler resources without requiring a prior Start().
// Safe to call on orchestrators that were only used for config/vault operations
// (e.g., in tests). Idempotent — calling Close after Stop is harmless.
func (o *Orchestrator) Close() {
	_ = o.scheduler.Stop()
}

// setIngesterAlive updates both the local stats and the Raft-replicated state.
// The ingester adapter calls this around each ingester run so the cluster/UI
// alive surface is unchanged after the V0 ingest loop's removal.
func (o *Orchestrator) setIngesterAlive(id glid.GLID, stats *IngesterStats, alive bool) {
	if stats != nil {
		stats.Alive.Store(alive)
	}
	if o.onIngesterAlive != nil {
		o.onIngesterAlive(id, alive)
	}
}

// RebuildMissingIndexes checks all sealed chunks and rebuilds indexes for any
// that are incomplete. This should be called before Start() to recover from
// interrupted index builds.
// RebuildMissingIndexes scans all sealed chunks and triggers index builds
// for any that are missing indexes. Builds run in the background using the
// orchestrator's indexWg, so this method returns immediately after launching
// the builds.
func (o *Orchestrator) RebuildMissingIndexes(ctx context.Context) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for vaultID, vault := range o.vaults {
		if vault == nil || vault.Instance == nil {
			continue
		}
		if err := o.rebuildVaultIndexes(ctx, vaultID, vault.Instance); err != nil {
			return err
		}
	}

	return nil
}

// rebuildVaultIndexes checks a single instance for sealed chunks with incomplete indexes.
func (o *Orchestrator) rebuildVaultIndexes(ctx context.Context, vaultID glid.GLID, vaultInst *VaultInstance) error {
	// Skip vaults where the post-seal pipeline handles indexes.
	if proc, ok := vaultInst.Chunks.(chunk.ChunkPostSealProcessor); ok {
		if !proc.HasIndexBuilders() {
			return nil
		}
	}

	metas, err := vaultInst.Chunks.List()
	if err != nil {
		return err
	}

	for _, meta := range metas {
		// Phase 3 (gastrolog-1huz5): rebuild indexes only for chunks
		// the FSM considers Sealed — Sealing chunks have no GLCB yet,
		// so the index builder would fail to read records.
		if vaultInst.OverlayFromFSM != nil {
			meta = vaultInst.OverlayFromFSM(meta)
		}
		if !meta.Sealed {
			continue
		}
		if meta.CloudBacked && vaultInst.IsFollower {
			continue // no local data — adopted via RegisterCloudBackedChunk
		}
		o.scheduleIndexRebuildIfNeeded(ctx, vaultID, vaultInst, meta)
	}
	return nil
}

func (o *Orchestrator) scheduleIndexRebuildIfNeeded(ctx context.Context, vaultID glid.GLID, vaultInst *VaultInstance, meta chunk.ChunkMeta) {
	complete, err := vaultInst.Indexes.IndexesComplete(meta.ID)
	if err != nil || complete {
		return
	}
	// Followers can host many replicated chunks; eagerly rebuilding every
	// missing index on each follower at startup causes N-way rebuild storms.
	// Keep bootstrap rebuilds on leaders only.
	if vaultInst.IsFollower {
		return
	}
	o.logger.Info("rebuilding missing indexes",
		"vault", vaultID, "chunk", meta.ID.String())
	name := fmt.Sprintf("index-rebuild:%s:%s:%s", vaultID, vaultInst.VaultID, meta.ID)
	runBuild := func(runCtx context.Context, chunkID chunk.ChunkID) error {
		return vaultInst.Indexes.BuildIndexes(runCtx, chunkID)
	}
	if err := o.scheduler.RunOnce(name, runBuild, ctx, meta.ID); err != nil {
		o.logger.Warn("failed to schedule index rebuild", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Rebuild missing indexes for chunk %s", meta.ID))
}
