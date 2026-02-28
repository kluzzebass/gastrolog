package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
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

	if o.filterSet == nil && len(o.vaults) > 1 {
		o.logger.Warn("no filters configured, messages will fan out to all vaults")
	}

	// Start shared scheduler (cron rotation, retention, and future scheduled tasks).
	o.scheduler.Start()

	// Launch ingester goroutines with per-ingester contexts.
	for id, r := range o.ingesters {
		recvCtx, recvCancel := context.WithCancel(ctx)
		o.ingesterCancels[id] = recvCancel
		meta := o.ingesterMeta[id]
		o.logger.Info("starting ingester", "id", id, "name", meta.Name, "type", meta.Type)
		o.ingesterWg.Go(func() { _ = r.Run(recvCtx, o.ingestCh) })
	}

	// Launch digest + write pipeline.
	o.digestWg.Go(func() { o.digestLoop(ctx) })
	o.writeWg.Go(func() { o.writeLoop() })

	return nil
}

// Stop cancels all ingesters, the digest/write pipeline, and in-flight index
// builds, then waits for everything to finish.
//
// Ordered shutdown:
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

	// Stop shared scheduler — waits for running jobs (index builds,
	// cron rotation, retention) to finish.
	_ = o.scheduler.Stop()

	o.mu.Lock()
	o.running = false
	o.cancel = nil
	o.done = nil
	o.ingestCh = nil
	o.digestedCh = nil
	// Clear per-ingester cancel functions.
	o.ingesterCancels = make(map[uuid.UUID]context.CancelFunc)
	o.mu.Unlock()

	return nil
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
	// Stamp identity attrs so records are traceable to the ingesting node and ingester.
	if o.localNodeID != "" || msg.IngesterID != "" {
		if msg.Attrs == nil {
			msg.Attrs = make(map[string]string, 2)
		}
		if o.localNodeID != "" {
			msg.Attrs["node_id"] = o.localNodeID
		}
		if msg.IngesterID != "" {
			msg.Attrs["ingester_id"] = msg.IngesterID
		}
	}

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
		err := o.ingest(dr.rec)

		// Track per-ingester stats.
		o.trackWriteStats(dr, err)

		// Send ack if requested.
		if dr.ack != nil {
			dr.ack <- err
		}
	}
}

// trackWriteStats updates per-ingester counters for a written record.
func (o *Orchestrator) trackWriteStats(dr digestedRecord, ingestErr error) {
	if dr.ingesterID == "" {
		return
	}
	id, parseErr := uuid.Parse(dr.ingesterID)
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
		cm := vault.Chunks
		im := vault.Indexes

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
					"vault", vaultID,
					"chunk", meta.ID.String())

				name := fmt.Sprintf("index-rebuild:%s:%s", vaultID, meta.ID)
				if err := o.scheduler.RunOnce(name, vault.Indexes.BuildIndexes, context.Background(), meta.ID); err != nil {
					o.logger.Warn("failed to schedule index rebuild", "name", name, "error", err)
				}
				o.scheduler.Describe(name, fmt.Sprintf("Rebuild missing indexes for chunk %s", meta.ID))
			}
		}
	}

	return nil
}
