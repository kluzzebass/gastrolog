package orchestrator

import (
	"context"

	"gastrolog/internal/chunk"
)

// Start launches all ingesters and the ingest loop.
// Each ingester runs in its own goroutine, emitting messages to a shared channel.
// The ingest loop receives messages, resolves identity, and routes to chunk managers.
// Start returns immediately; use Stop() to shut down.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.running {
		return ErrAlreadyRunning
	}

	// Create cancellable context for all ingesters and ingest loop.
	ctx, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.done = make(chan struct{})
	o.running = true

	// Create fresh index context for this run cycle.
	o.indexCtx, o.indexCancel = context.WithCancel(context.Background())

	// Create ingest channel.
	o.ingestCh = make(chan IngestMessage, o.ingestSize)

	// Log startup info.
	o.logger.Info("starting orchestrator",
		"stores", len(o.chunks),
		"ingesters", len(o.ingesters))

	if o.filterSet == nil && len(o.chunks) > 1 {
		o.logger.Warn("no filters configured, messages will fan out to all stores")
	}

	// Launch retention runners.
	for id, runner := range o.retention {
		id, runner := id, runner
		retCtx, retCancel := context.WithCancel(ctx)
		o.retentionCancels[id] = retCancel
		o.logger.Info("starting retention runner", "store", id)
		o.retentionWg.Go(func() { runner.run(retCtx) })
	}

	// Launch ingester goroutines with per-ingester contexts.
	for id, r := range o.ingesters {
		id, r := id, r // capture for closure
		recvCtx, recvCancel := context.WithCancel(ctx)
		o.ingesterCancels[id] = recvCancel
		o.logger.Info("starting ingester", "id", id)
		o.ingesterWg.Go(func() { r.Run(recvCtx, o.ingestCh) })
	}

	// Launch ingest loop.
	o.ingestLoopWg.Go(func() { o.ingestLoop(ctx) })

	return nil
}

// Stop cancels all ingesters, the ingest loop, and in-flight index builds,
// then waits for everything to finish.
func (o *Orchestrator) Stop() error {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return ErrNotRunning
	}
	cancel := o.cancel
	indexCancel := o.indexCancel
	ingestCh := o.ingestCh
	o.mu.Unlock()

	// Cancel all ingester contexts (both initial and dynamically added).
	o.mu.Lock()
	for _, recvCancel := range o.ingesterCancels {
		recvCancel()
	}
	// Cancel all retention runners.
	for _, retCancel := range o.retentionCancels {
		retCancel()
	}
	o.mu.Unlock()

	// Cancel main context (for ingest loop).
	cancel()

	// Cancel in-flight index builds.
	indexCancel()

	// Wait for ingesters to exit, then close the ingest channel.
	o.ingesterWg.Wait()
	close(ingestCh)

	// Wait for ingest loop to finish.
	o.ingestLoopWg.Wait()

	// Wait for index builds to exit.
	o.indexWg.Wait()

	// Wait for retention runners to exit.
	o.retentionWg.Wait()

	o.mu.Lock()
	o.running = false
	o.cancel = nil
	o.done = nil
	o.ingestCh = nil
	o.indexCtx = nil
	o.indexCancel = nil
	// Clear per-ingester cancel functions.
	o.ingesterCancels = make(map[string]context.CancelFunc)
	// Clear per-retention cancel functions.
	o.retentionCancels = make(map[string]context.CancelFunc)
	o.mu.Unlock()

	return nil
}

// ingestLoop receives messages from the ingest channel and routes them.
//
// Throughput: A single goroutine processes all messages sequentially.
// This is intentional because:
//   - Chunk append is serialized anyway (single writer per ChunkManager)
//   - Identity resolution is cheap (in-memory map lookup)
//   - Index scheduling is async (fire-and-forget goroutine)
//
// If this becomes a bottleneck, parallelization can be added later
// (e.g., worker pool with per-ChunkManager routing).
func (o *Orchestrator) ingestLoop(ctx context.Context) {
	defer close(o.done)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, but drain remaining messages from channel.
			// Channel will be closed after ingesters exit.
			for msg := range o.ingestCh {
				o.processMessage(msg)
			}
			return
		case msg, ok := <-o.ingestCh:
			if !ok {
				return
			}
			o.processMessage(msg)
		}
	}
}

// processMessage applies digesters then routes to chunk managers.
func (o *Orchestrator) processMessage(msg IngestMessage) {
	// Apply digester pipeline (enriches attrs based on message content).
	for _, d := range o.digesters {
		d.Digest(&msg)
	}

	// Construct record.
	// SourceTS comes from the ingester (parsed from log, zero if unknown).
	// IngestTS comes from the ingester (when message was received).
	// WriteTS is set by ChunkManager on append.
	// Attrs may have been enriched by digesters.
	rec := chunk.Record{
		SourceTS: msg.SourceTS,
		IngestTS: msg.IngestTS,
		Attrs:    msg.Attrs,
		Raw:      msg.Raw,
	}

	// Route to chunk managers (reuses existing Ingest logic).
	err := o.ingest(rec)

	// Send ack if requested.
	if msg.Ack != nil {
		msg.Ack <- err
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

	for storeID, cm := range o.chunks {
		im, ok := o.indexes[storeID]
		if !ok {
			continue
		}

		metas, err := cm.List()
		if err != nil {
			return err
		}

		for _, meta := range metas {
			if !meta.Sealed {
				continue
			}

			complete, err := im.IndexesComplete(meta.ID)
			if err != nil {
				return err
			}

			if !complete {
				o.logger.Info("rebuilding missing indexes",
					"store", storeID,
					"chunk", meta.ID.String())

				// Use the same indexWg as seal-triggered builds.
				storeID, chunkID, im := storeID, meta.ID, im
				o.indexWg.Go(func() {
					if err := im.BuildIndexes(ctx, chunkID); err != nil {
						o.logger.Error("failed to rebuild indexes",
							"store", storeID,
							"chunk", chunkID.String(),
							"error", err)
					}
				})
			}
		}
	}

	return nil
}
