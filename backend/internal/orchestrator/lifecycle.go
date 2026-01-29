package orchestrator

import (
	"context"

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

	// Create ingest channel.
	o.ingestCh = make(chan IngestMessage, o.ingestSize)

	// Launch receiver goroutines.
	for id, r := range o.receivers {
		go o.runReceiver(ctx, id, r)
	}

	// Launch ingest loop.
	go o.ingestLoop(ctx)

	return nil
}

// Stop cancels all receivers and the ingest loop, then waits for completion.
func (o *Orchestrator) Stop() error {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return ErrNotRunning
	}
	cancel := o.cancel
	done := o.done
	o.mu.Unlock()

	// Cancel context to stop receivers and ingest loop.
	cancel()

	// Wait for ingest loop to finish.
	<-done

	o.mu.Lock()
	o.running = false
	o.cancel = nil
	o.done = nil
	o.ingestCh = nil
	o.mu.Unlock()

	return nil
}

// runReceiver runs a single receiver, recovering from panics.
func (o *Orchestrator) runReceiver(ctx context.Context, id string, r Receiver) {
	// Receiver.Run blocks until ctx is cancelled or error.
	// Errors are currently ignored - future: add error callback or logging.
	_ = r.Run(ctx, o.ingestCh)
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
	now := o.now()
	rec := chunk.Record{
		WriteTS:  now,
		IngestTS: now,
		SourceID: sourceID,
		Raw:      msg.Raw,
	}

	// Route to chunk managers (reuses existing Ingest logic).
	_ = o.ingest(rec)
}
