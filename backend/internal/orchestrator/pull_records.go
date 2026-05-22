// Pull-by-EventID source-side handler for gastrolog-4t3y4.
//
// When a peer requests a set of EventIDs via ClusterService.PullRecords,
// the cluster server dispatches into PullSelectedRecords. This method
// opens a cursor on the local copy of (vault, chunk), filters by the
// EventID set, returns (scheduled, missing) synchronously to the puller,
// and dispatches the actual record push asynchronously over the existing
// per-vault chunk-replication stream via SendFillRecords / SendFillComplete.
//
// See docs/pull-records-design.md for the wire contract and gastrolog-4t3y4
// for scope.

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// pullFillMaxRecords caps the per-batch SendFillRecords frame size so no
// individual gRPC message exceeds the receive limit. Mirrors the cap used by
// ChunkReplicator.ImportSealedChunk (importRecordsMaxRecords).
const pullFillMaxRecords = 64

// pullFillTimeout bounds the asynchronous push of a fill sequence to a
// single puller. Generous because large pull requests under bandwidth
// contention can take noticeable time; the per-vault stream's own retry/
// backoff handles transient peer disconnects below this bound.
const pullFillTimeout = 5 * time.Minute

// PullSelectedRecords is the orchestrator-side handler for PullRecords.
// Validates the local chunk, scans it filtering by eventIDs, returns
// (scheduled, missing) immediately, and fires the asynchronous fill push
// via ChunkReplicator.
//
// Errors returned here propagate up to the cluster server as gRPC errors
// — the puller sees them as Internal status. Asynchronous fill errors
// (mid-push connection drop, source-side cursor failure) surface via
// ChunkReplicationFillComplete frames with a non-empty error string.
func (o *Orchestrator) PullSelectedRecords(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, eventIDs []chunk.EventID, requesterNodeID string) (scheduled, missing uint32, err error) {
	if o.chunkReplicator == nil {
		return 0, 0, errors.New("no chunk replicator configured")
	}
	if requesterNodeID == "" {
		return 0, 0, errors.New("requester node id is required")
	}
	if requesterNodeID == o.localNodeID {
		return 0, 0, errors.New("self-pull not supported (puller and source are the same node)")
	}

	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil || vault.Instance == nil {
		return 0, 0, fmt.Errorf("vault %s not found", vaultID)
	}
	cm := vault.Instance.Chunks
	if cm == nil {
		return 0, 0, fmt.Errorf("vault %s has no chunk manager", vaultID)
	}

	// First pass: open a cursor, scan, partition records into the requested
	// set vs. the leftover EventIDs the source doesn't have locally. We do
	// this synchronously so the puller's PullRecordsResponse carries
	// accurate scheduled/missing counts — the async fill push only sends
	// what the synchronous scan already located.
	wanted := make(map[chunk.EventID]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		wanted[id] = struct{}{}
	}

	cur, err := cm.OpenCursor(chunkID)
	if err != nil {
		return 0, 0, fmt.Errorf("open cursor for chunk %s: %w", chunkID, err)
	}
	matched := make([]chunk.Record, 0, len(eventIDs))
	for {
		rec, _, err := cur.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			_ = cur.Close()
			return 0, 0, fmt.Errorf("scan chunk %s: %w", chunkID, err)
		}
		if _, want := wanted[rec.EventID]; !want {
			continue
		}
		// Copy: the cursor's record may reference mmap regions invalidated
		// when the cursor closes.
		matched = append(matched, rec.Copy())
		delete(wanted, rec.EventID)
		if len(wanted) == 0 {
			break // all requested EventIDs found locally; stop early
		}
	}
	_ = cur.Close()

	scheduled = uint32(len(matched))     //nolint:gosec // bounded by len(eventIDs)
	missing = uint32(len(wanted))        //nolint:gosec // bounded by len(eventIDs)

	// Asynchronously push the matched records to the puller via the existing
	// per-vault chunk-replication stream. Detached from the request context
	// (the puller's PullRecords call returns as soon as scheduled/missing are
	// known); the fill push has its own lifetime governed by the per-vault
	// stream's cluster.ReplicationTimeout.
	if scheduled > 0 || missing == uint32(len(eventIDs)) { //nolint:gosec // bounded by len(eventIDs)
		// scheduled > 0: we have records to push.
		// missing == |eventIDs|: we have NOTHING to push but the puller
		//   still needs a FillComplete to know we're done; otherwise it
		//   waits indefinitely.
		go o.dispatchFillRecords(vaultID, chunkID, requesterNodeID, matched)
	}

	return scheduled, missing, nil
}

// FillSealedRecords is the receiver-side handler for FillRecords frames
// targeting Sealed-not-reconciled chunks (gastrolog-4t3y4). The cluster
// server's chunk-replication handler falls back here when the
// active/sealing append path rejects with ErrChunkSealed. This routine
// type-asserts the vault's chunk manager to chunk.SealedRepairer and
// dispatches; returns chunk.ErrNotImplemented when the manager doesn't
// implement SealedRepairer (e.g., memory / jsonl) so the puller's
// reconcile loop can log a TODO marker.
func (o *Orchestrator) FillSealedRecords(_ context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil || vault.Instance == nil {
		return fmt.Errorf("vault %s not found", vaultID)
	}
	cm := vault.Instance.Chunks
	if cm == nil {
		return fmt.Errorf("vault %s has no chunk manager", vaultID)
	}
	repairer, ok := cm.(chunk.SealedRepairer)
	if !ok {
		// Memory / jsonl chunk managers don't implement SealedRepairer;
		// surface ErrNotImplemented so the caller knows this isn't a
		// transient failure but a structural mismatch.
		return chunk.ErrNotImplemented
	}
	return repairer.FillSealed(chunkID, records)
}

// dispatchFillRecords pushes matched records to the puller in bounded
// batches followed by a FillComplete signal. Errors are logged but not
// surfaced to the synchronous PullRecords caller — they reach the puller
// via the FillComplete frame's error field, or via the per-vault stream's
// connection-drop semantics if the stream itself dies mid-push.
func (o *Orchestrator) dispatchFillRecords(vaultID glid.GLID, chunkID chunk.ChunkID, requesterNodeID string, records []chunk.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), pullFillTimeout)
	defer cancel()

	logger := o.logger.With(
		"component", "orchestrator",
		"flow", "pull-records",
		"vault", vaultID.String(),
		"chunk", chunkID.String(),
		"puller", requesterNodeID,
		"records", len(records),
	)

	var sent uint32
	// Batch the records into bounded frames so no individual gRPC message
	// exceeds the receive limit on the puller side.
	for i := 0; i < len(records); i += pullFillMaxRecords {
		end := min(i+pullFillMaxRecords, len(records))
		batch := records[i:end]
		lastBatch := end == len(records)
		if err := o.chunkReplicator.SendFillRecords(ctx, requesterNodeID, vaultID, chunkID, batch, lastBatch); err != nil {
			// Surface the abort to the puller via FillComplete with error;
			// best-effort, ignore errors on FillComplete itself (the stream
			// may already be torn down).
			logger.Warn("pull: send fill records failed; signaling abort", "error", err, "batch_start", i)
			_ = o.chunkReplicator.SendFillComplete(ctx, requesterNodeID, vaultID, chunkID, sent, err.Error())
			return
		}
		sent += uint32(len(batch)) //nolint:gosec // bounded by len(records)
	}

	// All records sent (or zero records to send). Close the sequence with
	// FillComplete so the puller can fire CmdAckPull when 37k2b-e wires it.
	if err := o.chunkReplicator.SendFillComplete(ctx, requesterNodeID, vaultID, chunkID, sent, ""); err != nil {
		logger.Warn("pull: send fill complete failed", "error", err, "records_sent", sent)
	}
}
