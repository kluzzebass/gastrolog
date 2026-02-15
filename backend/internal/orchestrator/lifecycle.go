package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
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

	// Create ingest channel.
	o.ingestCh = make(chan IngestMessage, o.ingestSize)

	// Log startup info.
	o.logger.Info("starting orchestrator",
		"stores", len(o.stores),
		"ingesters", len(o.ingesters))

	if o.filterSet == nil && len(o.stores) > 1 {
		o.logger.Warn("no filters configured, messages will fan out to all stores")
	}

	// Start shared scheduler (cron rotation, retention, and future scheduled tasks).
	o.scheduler.Start()

	// Launch ingester goroutines with per-ingester contexts.
	for id, r := range o.ingesters {
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
	ingestCh := o.ingestCh
	o.mu.Unlock()

	// Cancel all ingester contexts (both initial and dynamically added).
	o.mu.Lock()
	for _, recvCancel := range o.ingesterCancels {
		recvCancel()
	}
	o.mu.Unlock()

	// Cancel main context (for ingest loop).
	cancel()

	// Wait for ingesters to exit, then close the ingest channel.
	o.ingesterWg.Wait()
	close(ingestCh)

	// Wait for ingest loop to finish.
	o.ingestLoopWg.Wait()

	// Stop shared scheduler — waits for running jobs (index builds,
	// cron rotation, retention) to finish.
	o.scheduler.Stop()

	o.mu.Lock()
	o.running = false
	o.cancel = nil
	o.done = nil
	o.ingestCh = nil
	// Clear per-ingester cancel functions.
	o.ingesterCancels = make(map[uuid.UUID]context.CancelFunc)
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

	// Track per-ingester stats.
	if idStr := msg.Attrs["ingester_id"]; idStr != "" {
		if id, parseErr := uuid.Parse(idStr); parseErr == nil {
			if stats := o.ingesterStats[id]; stats != nil {
				stats.MessagesIngested.Add(1)
				stats.BytesIngested.Add(int64(len(msg.Raw)))
				if err != nil {
					stats.Errors.Add(1)
				}
			}
		}
	}

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

	for storeID, store := range o.stores {
		if store == nil {
			continue
		}
		cm := store.Chunks
		im := store.Indexes

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

				name := fmt.Sprintf("index-rebuild:%s:%s", storeID, meta.ID)
				if err := o.scheduler.RunOnce(name, store.Indexes.BuildIndexes, context.Background(), meta.ID); err != nil {
					o.logger.Warn("failed to schedule index rebuild", "name", name, "error", err)
				}
				o.scheduler.Describe(name, fmt.Sprintf("Rebuild missing indexes for chunk %s", meta.ID))
			}
		}
	}

	return nil
}
