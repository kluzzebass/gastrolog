package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"math/rand/v2"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
)

// digestedRecord is the intermediate type passed from digestLoop to writeLoop.
type digestedRecord struct {
	rec        chunk.Record
	ack        chan<- error
	ingesterID string
	rawLen     int // original message raw length for stats
}

// Start launches all ingesters and the digest/write pipeline.
// Each ingester runs in its own goroutine, emitting messages to a shared channel.
// The digest loop receives messages, resolves identity, runs digesters, and builds
// records. The write loop receives digested records and appends them to vaults.
// Start returns immediately; use Stop() to shut down.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.running {
		return ErrAlreadyRunning
	}

	// Create cancellable context for all ingesters and digest loop.
	ctx, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.done = make(chan struct{})
	o.running = true

	// Create ingest and intermediate channels.
	o.ingestCh = make(chan IngestMessage, o.ingestSize)
	o.digestedCh = make(chan digestedRecord, o.ingestSize)

	// Log startup info.
	o.logger.Info("starting orchestrator",
		"vaults", len(o.vaults),
		"ingesters", len(o.ingesters))

	if o.filterSet == nil && len(o.vaults) > 0 {
		o.logger.Warn("no routes configured, ingested records will be dropped")
	}

	// Start shared scheduler (cron rotation, retention, and future scheduled tasks).
	o.scheduler.Start()

	// Probes for chanwatch and PressureGate.
	ingestProbe := func() (int, int) {
		return len(o.ingestCh), cap(o.ingestCh)
	}
	digestedProbe := func() (int, int) {
		return len(o.digestedCh), cap(o.digestedCh)
	}

	// Pressure gate: ingesters consult this to throttle when the pipeline
	// is backed up. Hysteresis thresholds: elevated at 80%, critical at 95%,
	// release at 50%. Transitions are reported via alerts — NOT slog —
	// to avoid a feedback loop where the self-ingester captures throttle
	// messages and adds to the pressure.
	gate := chanwatch.NewPressureGate(chanwatch.DefaultThresholds())
	gate.AddProbe("ingestCh", ingestProbe)
	gate.AddProbe("digestedCh", digestedProbe)
	if ac, ok := o.alerts.(*alert.Collector); ok {
		gate.AddOnChange(func(tr chanwatch.PressureTransition) {
			if tr.To == chanwatch.PressureNormal {
				ac.Clear("ingest-pressure")
				return
			}
			sev := alert.Warning
			if tr.To == chanwatch.PressureCritical {
				sev = alert.Error
			}
			ac.Set(
				"ingest-pressure",
				sev, "orchestrator",
				fmt.Sprintf("Ingest pipeline pressure %s (%s at %d%%)",
					tr.To, tr.Cause, int(tr.Ratio*100)),
			)
		})
	}
	// o.mu is already held by Start(); assign directly.
	o.pressureGate = gate

	// Include cross-node forward channels in pipeline-wide pressure
	// classification so ingesters throttle when remote forwarding is
	// backed up, not only when local ingest/digest buffers fill. See
	// gastrolog-27zvt.
	if o.forwarder != nil {
		o.forwarder.RegisterPressureGate(gate)
	}

	// Launch ingester goroutines with per-ingester contexts. Inject the
	// pressure gate into any ingester that implements PressureAware so it
	// can throttle on backpressure.
	for id, r := range o.ingesters {
		if pa, ok := r.(PressureAware); ok {
			pa.SetPressureGate(gate)
		}
		recvCtx, recvCancel := context.WithCancel(ctx)
		o.ingesterCancels[id] = recvCancel
		meta := o.ingesterMeta[id]
		o.logger.Info("starting ingester", "id", id, "name", meta.Name, "type", meta.Type)
		o.ingesterWg.Go(func() { o.runIngester(id, r, recvCtx, o.ingestCh) })
	}

	// Launch digest + write pipeline.
	o.digestWg.Go(func() { o.digestLoop(ctx) })
	o.writeWg.Go(func() { o.writeLoop() })

	// Channel pressure watchdog — kept for the slog-based alerts at 90%
	// (separate codepath from the hysteresis gate used for throttling).
	cw := chanwatch.New(o.logger, 1*time.Second)
	if ac, ok := o.alerts.(*alert.Collector); ok {
		cw.SetAlerts(ac)
	}
	cw.Watch("ingestCh", ingestProbe, 0.9)
	cw.Watch("digestedCh", digestedProbe, 0.9)
	o.auxWg.Go(func() { cw.Run(ctx) })

	// Start the pressure gate after everything else is wired.
	o.auxWg.Go(func() { gate.Run(ctx, 200*time.Millisecond) })

	// Periodic per-tier rate alert evaluator (gastrolog-47qyw). Evaluates
	// rotation and retention rates against thresholds every 5 seconds and
	// raises/clears alerts as needed.
	o.auxWg.Go(func() { o.runRateAlertEvaluator(ctx, 5*time.Second) })

	return nil
}

// runRateAlertEvaluator periodically evaluates rotation and retention rate
// alerters. Exits when ctx is cancelled.
func (o *Orchestrator) runRateAlertEvaluator(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := o.now()
			o.rotationRates.Evaluate(now)
			o.retentionRates.Evaluate(now)
			o.evaluateCloudHealth()
		}
	}
}

// Stop cancels all ingesters, the digest/write pipeline, and in-flight index
// builds, then waits for everything to finish.
//
// Ordered shutdown:
//  0. BeginShutdown on the shared phase (if wired) → fast-path skip in
//     fireAndForgetRemote / sealRemoteFollowers so the drain pipeline
//     doesn't spam peers that are going down alongside us.
//  1. Cancel ingester contexts → ingesterWg.Wait() → close ingestCh
//  2. digestWg.Wait() (drains remaining messages) → close digestedCh
//  3. writeWg.Wait() (drains remaining records) → close done
func (o *Orchestrator) Stop() error {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return ErrNotRunning
	}
	cancel := o.cancel
	ingestCh := o.ingestCh
	digestedCh := o.digestedCh
	o.mu.Unlock()

	// Stage 0: flip the shutdown phase BEFORE any drain work so that
	// fireAndForgetRemote / sealRemoteFollowers skip their remote calls
	// while we drain buffered records through the pipeline. Idempotent
	// if the top-level shutdown already flipped it; safe to call with a
	// nil phase (single-node tests). See gastrolog-1e5ke.
	if o.phase != nil {
		o.phase.BeginShutdown("orchestrator: cancelling ingesters")
	}

	// Cancel all ingester contexts (both initial and dynamically added).
	o.mu.Lock()
	for _, recvCancel := range o.ingesterCancels {
		recvCancel()
	}
	o.mu.Unlock()

	// Cancel main context (for digest loop).
	cancel()

	// Stage 1: Wait for ingesters to exit, then close the ingest channel.
	o.ingesterWg.Wait()
	close(ingestCh)

	// Stage 2: Wait for digest loop to drain, then close the intermediate channel.
	o.digestWg.Wait()
	close(digestedCh)

	// Stage 3: Wait for write loop to drain remaining records.
	o.writeWg.Wait()

	// Stage 4: Wait for in-flight ack-gated replication goroutines.
	o.ackWg.Wait()

	// Stage 5: Wait for auxiliary goroutines (watchdog, etc.).
	o.auxWg.Wait()

	// Stop shared scheduler — waits for running jobs (index builds,
	// cron rotation, retention) to finish.
	_ = o.scheduler.Stop()

	// Stop all per-tier leader loops (after the scheduler so reconcile
	// passes don't fight retention deletes during shutdown).
	o.vaultCtlLeaders.StopAll()

	o.mu.Lock()
	o.running = false
	o.cancel = nil
	o.done = nil
	o.ingestCh = nil
	o.digestedCh = nil
	// Clear per-ingester cancel functions.
	o.ingesterCancels = make(map[glid.GLID]context.CancelFunc)
	o.mu.Unlock()

	return nil
}

// Close releases scheduler resources without requiring a prior Start().
// Safe to call on orchestrators that were only used for config/vault operations
// (e.g., in tests). Idempotent — calling Close after Stop is harmless.
func (o *Orchestrator) Close() {
	_ = o.scheduler.Stop()
}

// runIngester executes a single ingester with panic recovery so that a
// misbehaving ingester cannot crash the entire process.
//
// Passive (listener) ingesters retry on failure with 3–5s jitter — port-bind
// errors are recoverable when another process releases the port or a
// co-located node dies. Active ingesters exit on first error.
func (o *Orchestrator) runIngester(id glid.GLID, r Ingester, ctx context.Context, out chan<- IngestMessage) {
	defer func() {
		if v := recover(); v != nil {
			o.logger.Error("ingester panicked", "id", id, "panic", v)
		}
	}()

	meta := o.ingesterMeta[id]
	stats := o.ingesterStats[id]
	for {
		o.setIngesterAlive(id, stats, true)
		err := o.runWithCheckpoints(ctx, id, r, out)
		o.setIngesterAlive(id, stats, false)
		if ctx.Err() != nil || !meta.Passive {
			return
		}
		delay := 3*time.Second + time.Duration(rand.Int64N(int64(2*time.Second))) //nolint:gosec // G404: jitter for retry delay, not security-sensitive
		o.logger.Warn("passive ingester failed, retrying", "id", id, "name", meta.Name, "error", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// runWithCheckpoints runs the ingester and, if it implements Checkpointable,
// saves checkpoints every 5 seconds and once on exit.
func (o *Orchestrator) runWithCheckpoints(ctx context.Context, id glid.GLID, r Ingester, out chan<- IngestMessage) error {
	cp, isCheckpointable := r.(Checkpointable)
	if !isCheckpointable || o.onIngesterCheckpoint == nil {
		return r.Run(ctx, out)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx, out) }()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case err := <-errCh:
			o.saveCheckpointFrom(id, cp)
			return err
		case <-ticker.C:
			o.saveCheckpointFrom(id, cp)
		}
	}
}

// saveCheckpointFrom saves checkpoint data from a Checkpointable ingester.
func (o *Orchestrator) saveCheckpointFrom(id glid.GLID, cp Checkpointable) {
	data, err := cp.SaveCheckpoint()
	if err != nil {
		o.logger.Error("ingester checkpoint save failed", "id", id, "error", err)
		return
	}
	if len(data) > 0 {
		o.onIngesterCheckpoint(id, data)
	}
}

// setIngesterAlive updates both the local stats and the Raft-replicated state.
func (o *Orchestrator) setIngesterAlive(id glid.GLID, stats *IngesterStats, alive bool) {
	if stats != nil {
		stats.Alive.Store(alive)
	}
	if o.onIngesterAlive != nil {
		o.onIngesterAlive(id, alive)
	}
}

// digestLoop reads IngestMessages, stamps identity, runs the digester chain,
// builds chunk.Records, and forwards them to digestedCh.
//
// On context cancellation it drains remaining messages from ingestCh so that
// every message gets digested before the channel is closed.
func (o *Orchestrator) digestLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Context cancelled — drain remaining messages.
			// ingestCh will be closed after ingesters exit.
			for msg := range o.ingestCh {
				o.digestAndForward(msg)
			}
			return
		case msg, ok := <-o.ingestCh:
			if !ok {
				return
			}
			o.digestAndForward(msg)
		}
	}
}

// digestAndForward digests a single message and sends the result to digestedCh.
func (o *Orchestrator) digestAndForward(msg IngestMessage) {
	// Apply digester pipeline (enriches attrs based on message content).
	// NodeID lives on EventID as a first-class field (gastrolog-1k3l9);
	// the orchestrator no longer stamps it as an attribute.
	for _, d := range o.digesters {
		d.Digest(&msg)
	}

	// Construct record.
	// SourceTS comes from the ingester (parsed from log, zero if unknown).
	// IngestTS comes from the ingester (when message was received).
	// WriteTS is set by ChunkManager on append.
	// Attrs may have been enriched by digesters.
	rec := chunk.Record{
		SourceTS:       msg.SourceTS,
		IngestTS:       msg.IngestTS,
		Attrs:          msg.Attrs,
		Raw:            msg.Raw,
		WaitForReplica: msg.Ack != nil,
	}

	// Assign EventID when ingester identity is available.
	if msg.IngesterID != "" {
		seq := o.ingestSeqs[msg.IngesterID]
		o.ingestSeqs[msg.IngesterID] = seq + 1
		ingesterUUID, err := glid.ParseUUID(msg.IngesterID)
		if err == nil {
			rec.EventID = chunk.EventID{
				IngesterID: ingesterUUID,
				NodeID:     o.localNodeIDGLID,
				IngestTS:   msg.IngestTS,
				IngestSeq:  seq,
			}
		}
	}

	o.digestedCh <- digestedRecord{
		rec:        rec,
		ack:        msg.Ack,
		ingesterID: msg.IngesterID,
		rawLen:     len(msg.Raw),
	}
}

// writeLoop reads digested records, appends them to vaults, tracks stats,
// and sends acks. It exits when digestedCh is closed.
func (o *Orchestrator) writeLoop() {
	defer close(o.done)

	for dr := range o.digestedCh {
		// Filter to chunk managers (reuses existing Ingest logic).
		pa, err := o.ingest(dr.rec)
		if err != nil {
			o.logger.Error("write failed", "error", err, "ingester", dr.ingesterID)
		}

		// Track per-ingester stats.
		o.trackWriteStats(dr, err)

		// Send ack if requested.
		if dr.ack != nil {
			if err != nil || pa.isEmpty() {
				// Write failed or no sync work — ack immediately.
				dr.ack <- err
			} else {
				// Ack-gated: run the sync work (local follower
				// replication + cross-node forward) in a goroutine
				// so the writeLoop isn't blocked by network round-trips.
				o.ackWg.Go(func() {
					o.ackAfterReplication(dr.ack, pa, dr.rec)
				})
			}
		}
	}
}

// trackWriteStats updates per-ingester counters for a written record.
func (o *Orchestrator) trackWriteStats(dr digestedRecord, ingestErr error) {
	if dr.ingesterID == "" {
		return
	}
	id, parseErr := glid.ParseUUID(dr.ingesterID)
	if parseErr != nil {
		return
	}
	stats := o.ingesterStats[id]
	if stats == nil {
		return
	}
	stats.MessagesIngested.Add(1)
	stats.BytesIngested.Add(int64(dr.rawLen))
	if ingestErr != nil {
		stats.Errors.Add(1)
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
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
			if err := o.rebuildTierIndexes(ctx, vaultID, tier); err != nil {
				return err
			}
		}
	}

	return nil
}

// rebuildTierIndexes checks a single tier for sealed chunks with incomplete indexes.
func (o *Orchestrator) rebuildTierIndexes(ctx context.Context, vaultID glid.GLID, tier *TierInstance) error {
	// Skip tiers where the post-seal pipeline handles indexes.
	if proc, ok := tier.Chunks.(chunk.ChunkPostSealProcessor); ok {
		if !proc.HasIndexBuilders() {
			return nil
		}
	}

	metas, err := tier.Chunks.List()
	if err != nil {
		return err
	}

	for _, meta := range metas {
		if !meta.Sealed {
			continue
		}
		if meta.CloudBacked && tier.IsFollower {
			continue // no local data — adopted via RegisterCloudChunk
		}
		o.scheduleIndexRebuildIfNeeded(ctx, vaultID, tier, meta)
	}
	return nil
}

func (o *Orchestrator) scheduleIndexRebuildIfNeeded(ctx context.Context, vaultID glid.GLID, tier *TierInstance, meta chunk.ChunkMeta) {
	complete, err := tier.Indexes.IndexesComplete(meta.ID)
	if err != nil || complete {
		return
	}
	// Followers can host many replicated chunks; eagerly rebuilding every
	// missing index on each follower at startup causes N-way rebuild storms.
	// Keep bootstrap rebuilds on leaders only.
	if tier.IsFollower {
		return
	}
	o.logger.Info("rebuilding missing indexes",
		"vault", vaultID, "tier", tier.TierID, "chunk", meta.ID.String())
	name := fmt.Sprintf("index-rebuild:%s:%s:%s", vaultID, tier.TierID, meta.ID)
	runBuild := func(runCtx context.Context, chunkID chunk.ChunkID) error {
		return tier.Indexes.BuildIndexes(runCtx, chunkID)
	}
	if err := o.scheduler.RunOnce(name, runBuild, ctx, meta.ID); err != nil {
		o.logger.Warn("failed to schedule index rebuild", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Rebuild missing indexes for chunk %s", meta.ID))
}
