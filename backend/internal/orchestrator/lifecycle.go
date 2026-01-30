package orchestrator

import (
	"context"
	"sync"

	"gastrolog/internal/chunk"
)

// Start launches all receivers and the ingest loop.
// Each receiver runs in its own goroutine, emitting messages to a shared channel.
// The ingest loop receives messages, resolves identity, and routes to chunk managers.
// Start returns immediately; use Stop() to shut down.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.running {
		return ErrAlreadyRunning
	}

	// Create cancellable context for all receivers and ingest loop.
	ctx, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.done = make(chan struct{})
	o.running = true

	// Create fresh index context for this run cycle.
	o.indexCtx, o.indexCancel = context.WithCancel(context.Background())

	// Create ingest channel.
	o.ingestCh = make(chan IngestMessage, o.ingestSize)

	// Launch receiver goroutines.
	for _, r := range o.receivers {
		r := r // capture for closure
		o.receiverWg.Go(func() { r.Run(ctx, o.ingestCh) })
	}

	// Launch ingest loop.
	o.ingestLoopWg.Go(func() { o.ingestLoop(ctx) })

	return nil
}

// Stop cancels all receivers, the ingest loop, and in-flight index builds,
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

	// Cancel receivers and ingest loop.
	cancel()

	// Cancel in-flight index builds.
	indexCancel()

	// Wait for receivers to exit, then close the ingest channel.
	o.receiverWg.Wait()
	close(ingestCh)

	// Wait for ingest loop to finish.
	o.ingestLoopWg.Wait()

	// Wait for index builds to exit.
	o.indexWg.Wait()

	o.mu.Lock()
	o.running = false
	o.cancel = nil
	o.done = nil
	o.ingestCh = nil
	o.indexCtx = nil
	o.indexCancel = nil
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
			// Channel will be closed after receivers exit.
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

// processMessage resolves identity and routes to chunk managers.
func (o *Orchestrator) processMessage(msg IngestMessage) {
	// Resolve source identity.
	var sourceID chunk.SourceID
	if o.sources != nil {
		sourceID = o.sources.Resolve(msg.Attrs)
	}

	// Construct record.
	// IngestTS comes from the receiver (when message was received).
	// WriteTS is set by ChunkManager on append.
	rec := chunk.Record{
		IngestTS: msg.IngestTS,
		SourceID: sourceID,
		Raw:      msg.Raw,
	}

	// Route to chunk managers (reuses existing Ingest logic).
	_ = o.ingest(rec)
}

// RebuildMissingIndexes checks all sealed chunks and rebuilds indexes for any
// that are incomplete. This should be called before Start() to recover from
// interrupted index builds.
func (o *Orchestrator) RebuildMissingIndexes(ctx context.Context) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(o.chunks))

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
				wg.Add(1)
				go func(storeID string, chunkID chunk.ChunkID) {
					defer wg.Done()
					o.logger.Info("rebuilding missing indexes",
						"store", storeID,
						"chunk", chunkID.String())
					if err := im.BuildIndexes(ctx, chunkID); err != nil {
						o.logger.Error("failed to rebuild indexes",
							"store", storeID,
							"chunk", chunkID.String(),
							"error", err)
						errCh <- err
					}
				}(storeID, meta.ID)
			}
		}
	}

	wg.Wait()
	close(errCh)

	// Return first error if any.
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}
