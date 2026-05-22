package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
)

// Ingest filters a record to matching chunk managers.
// If a chunk is sealed as a result of the append, compression and index
// builds are scheduled asynchronously via appendRecord.
//
// This is the direct ingestion API for pre-constructed records.
// For ingester-based ingestion, use Start() which runs an ingest loop
// that receives IngestMessages, resolves identity, and calls this internally.
//
// All record writes (local ingest, cluster-forwarded, and import) flow
// through appendRecord, which handles seal detection and post-seal work.
//
// Error semantics: This is fan-out with partial failure. If CM A succeeds
// and CM B fails, the record is persisted in A but not B, and the error
// from B is returned. There is no rollback.
func (o *Orchestrator) Ingest(rec chunk.Record) error {
	return o.IngestWithSource(rec, SourceContext{Kind: SourceIngest})
}

// IngestWithSource ingests a record while tagging it with the given
// SourceContext. The synthetic attrs (`_source`, `_ingester`, etc.) are
// overlaid on rec.Attrs only at routing-evaluation time — they don't
// persist with the record.
//
// gastrolog-4kkoo (Phase 5): callers that have the ingester or
// retention-source identity should use this entry point. Direct
// callers without source context fall through to Ingest with
// {Kind: SourceIngest, IngesterID: zero}.
func (o *Orchestrator) IngestWithSource(rec chunk.Record, src SourceContext) error {
	pa, err := o.ingestWithSource(rec, src)
	if err != nil {
		return err
	}
	o.dispatchFanOutAsync(pa, rec)
	return nil
}

// ingest is the internal ingest implementation. Backwards-compatible
// shim — defaults source to {Kind: SourceIngest} for callers that
// haven't migrated to ingestWithSource.
func (o *Orchestrator) ingest(rec chunk.Record) (*pendingAcks, error) {
	return o.ingestWithSource(rec, SourceContext{Kind: SourceIngest})
}

// ingestWithSource is the source-aware ingest path. It threads a
// SourceContext through to ingestLocked so synthetic attributes
// (_source, _ingester, _vault, _reason) drive route evaluation.
//
// Returns pendingAcks bundling the sync work an ack-gated record triggers:
// fan-out W-of-N to chunk receivers, plus cross-node forwarding of records
// matched to remote vaults. Both task kinds must complete before the ack
// is delivered to the ingester. For non-ack-gated records that match a
// remote vault, syncForwards is populated; the caller must run
// flushRecordRouteForwards (outside o.mu) so the forward buffer can apply
// backpressure instead of dropping. See gastrolog-27zvt.
func (o *Orchestrator) ingestWithSource(rec chunk.Record, src SourceContext) (*pendingAcks, error) {
	return o.ingestLocked(rec, src)
}

// ingestLocked is the mu-protected portion of ingest. It returns the
// pendingAcks for ack-gated sync work.
//
// gastrolog-4kkoo (Phase 5): src carries the synthetic-attribute fields
// (_source, _ingester, _vault, _reason) that route expressions can
// match against. The synthetic overlay is applied per match call —
// rec.Attrs itself is not mutated.
func (o *Orchestrator) ingestLocked(rec chunk.Record, src SourceContext) (*pendingAcks, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	o.routeStats.Ingested.Add(1)

	if len(o.vaults) == 0 && o.forwarder == nil {
		o.routeStats.Dropped.Add(1)
		return nil, ErrNoChunkManagers
	}

	if o.routeSet == nil {
		o.routeStats.Dropped.Add(1)
		return nil, nil // No routes configured — drop the record.
	}

	matches := o.routeSet.MatchWithSource(rec.Attrs, src)
	if len(matches) == 0 {
		o.routeStats.Dropped.Add(1)
		return nil, nil
	}

	// Write records first, then update stats only on success.
	routed := false
	var pa *pendingAcks
	for _, t := range matches {
		taskAdded, err := o.dispatchRouted(&pa, t.VaultID, rec)
		if err != nil {
			if errors.Is(err, ErrVaultDisabled) {
				continue // Skip disabled vaults during ingestion.
			}
			return pa, err
		}
		if !taskAdded {
			continue
		}
		vs := o.getOrCreateVaultRouteStats(t.VaultID)
		vs.Matched.Add(1)
		if t.RouteID != glid.Nil {
			rs := o.getOrCreatePerRouteStats(t.RouteID)
			rs.Matched.Add(1)
		}
		routed = true
	}
	if routed {
		o.routeStats.Routed.Add(1)
	}
	return pa, nil
}

// dispatchRouted resolves a routed record to its destination under the
// fan-out data plane (gastrolog-4w8sv resolution): the originator
// dispatches directly to every Receiving member of the vault's active
// chunk, regardless of whether this node has a local vault instance.
//
//   - When self is in Receiving for the active chunk: local append happens
//     via appendLocal (which also builds the fan-out task targeting the
//     remaining Receiving members and accounts for self-ack via W-1).
//   - When self is NOT in Receiving (this node is just the ingest entry
//     point, or has no placement for the vault): buildFanOutTask is called
//     directly so the dispatch targets every Receiving member with full W.
//
// Returns (true, nil) when a fan-out task (with or without local append)
// was added to pendingAcks; (false, nil) when the record can't be
// dispatched (no active chunk in the FSM yet) and the caller should
// continue to the next match. Errors propagate from the local-append
// path.
//
// Caller holds o.mu.RLock.
func (o *Orchestrator) dispatchRouted(pa **pendingAcks, vaultID glid.GLID, rec chunk.Record) (bool, error) {
	chunkID, placement := o.lookupActivePlacement(vaultID)
	if chunkID == (chunk.ChunkID{}) || placement == nil {
		// No active chunk yet in the FSM. Falls back to local append
		// only when this node has a vault instance; that path opens
		// the first chunk via RotationCoordinator and Raft-mediates
		// the chunk-ID assignment. If this node has no local instance
		// either, the record has nowhere to land — drop.
		if _, ok := o.vaults[vaultID]; !ok {
			o.routeStats.Dropped.Add(1)
			return false, nil
		}
		fanOut, err := o.appendLocal(vaultID, rec)
		if err != nil {
			return false, err
		}
		if fanOut != nil {
			*pa = (*pa).addFanOut(*fanOut)
		}
		return true, nil
	}

	if placement.HasReceiving(o.localNodeID) {
		// Self is in Receiving — local append + fan-out to the other
		// Receiving members. appendLocal calls buildFanOutTask
		// internally and the resulting task has W decremented to
		// reflect the self-ack.
		fanOut, err := o.appendLocal(vaultID, rec)
		if err != nil {
			return false, err
		}
		if fanOut != nil {
			*pa = (*pa).addFanOut(*fanOut)
		}
		return true, nil
	}

	// Self is NOT in Receiving for this chunk — pure cross-node fan-out.
	// Build the task externally with the looked-up placement so the
	// dispatcher targets every Receiving member with full W (no self-ack
	// to subtract).
	fanOut := o.buildFanOutTask(vaultID, chunkID, placement, rec)
	if fanOut == nil {
		// Defensive: lookupActivePlacement returned a placement with
		// an empty Receiving. Drop — there's nobody to send to.
		o.routeStats.Dropped.Add(1)
		return false, nil
	}
	*pa = (*pa).addFanOut(*fanOut)
	return true, nil
}

// pendingAcks bundles the sync work that an ack-gated record triggers —
// the fan-out W-of-N writes to the active chunk's Receiving members.
// Must complete before the ack is delivered to the ingester.
//
// Under fan-out (gastrolog-mqxo4) cross-node forwarding via the legacy
// forwarder is no longer used in the routing path: every record goes
// through fan-out, whether or not the originating node has a local
// vault instance. The old pa.forwards / pa.syncForwards fields and the
// forwardTask type are gone with this refactor.
//
// Nil receiver is treated as empty; helpers lazy-init so callers don't
// have to check before appending.
type pendingAcks struct {
	fanOut []fanOutTask // fan-out W-of-N; ackAfterReplication waits on these
}

func (p *pendingAcks) addFanOut(t fanOutTask) *pendingAcks {
	if p == nil {
		p = &pendingAcks{}
	}
	p.fanOut = append(p.fanOut, t)
	return p
}

// isEmpty reports whether there is any sync work to wait on before acking.
func (p *pendingAcks) isEmpty() bool {
	return p == nil || len(p.fanOut) == 0
}

// getOrCreateVaultRouteStats returns the per-vault route stats, creating if needed.
func (o *Orchestrator) getOrCreateVaultRouteStats(vaultID glid.GLID) *VaultRouteStats {
	if v, ok := o.vaultRouteStats.Load(vaultID); ok {
		return v.(*VaultRouteStats)
	}
	v, _ := o.vaultRouteStats.LoadOrStore(vaultID, &VaultRouteStats{})
	return v.(*VaultRouteStats)
}

// getOrCreatePerRouteStats returns the per-route stats, creating if needed.
func (o *Orchestrator) getOrCreatePerRouteStats(routeID glid.GLID) *PerRouteStats {
	if v, ok := o.perRouteStats.Load(routeID); ok {
		return v.(*PerRouteStats)
	}
	v, _ := o.perRouteStats.LoadOrStore(routeID, &PerRouteStats{})
	return v.(*PerRouteStats)
}

// appendLocal appends a record to a local vault and returns the
// fanOutTask the caller threads through to ackAfterReplication.
//
// MUST be called with o.mu held.
func (o *Orchestrator) appendLocal(vaultID glid.GLID, rec chunk.Record) (*fanOutTask, error) {
	_, _, fanOut, err := o.appendRecord(vaultID, rec)
	if err != nil {
		o.logger.Error("append to vault failed", "vault", vaultID, "error", err)
	}
	return fanOut, err
}

// dispatchFanOutAsync fires pa.fanOut tasks in the background. Used by
// non-ack-gated ingest paths: the caller does not wait for replicas, but
// receivers still need the records or seal-time reconcile becomes the
// only path to durability. Ack-gated callers fire the same tasks through
// ackAfterReplication and BLOCK on completion before signalling the ack
// channel — this helper is the fire-and-forget twin.
func (o *Orchestrator) dispatchFanOutAsync(pa *pendingAcks, rec chunk.Record) {
	if pa == nil || len(pa.fanOut) == 0 {
		return
	}
	if o.shuttingDown() {
		return
	}
	for _, t := range pa.fanOut {
		o.ackWg.Go(func() {
			_ = o.runFanOut(context.Background(), &t, rec)
		})
	}
}

// postSealWork schedules the post-seal pipeline for a newly sealed chunk.
// Safe to call from any context (cron rotation, background sweep, etc.) —
// acquires the orchestrator lock internally.
func (o *Orchestrator) postSealWork(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	o.schedulePostSeal(vaultID, cm, chunkID)
	// Notify WatchChunks subscribers: the chunk's sealed flag has changed.
	// Fetch the post-seal meta so the event carries the final state instead
	// of forcing a refetch on the client.
	if meta, err := cm.Meta(chunkID); err == nil {
		o.EmitChunkSealed(vaultID, meta)
	} else {
		o.NotifyChunkChange() // fall back to bare wake-up if meta lookup failed
	}
}

// schedulePostSeal schedules the post-seal pipeline (compress → index →
// upload) for a chunk that just rotated. The fan-out data plane already
// replicated every record to each Receiver as it was appended, so seal
// time does not need to push the chunk anywhere — peers seal their own
// copy when they apply CmdSealChunk from vault-ctl Raft.
func (o *Orchestrator) schedulePostSeal(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	processor, ok := cm.(chunk.ChunkPostSealProcessor)
	if !ok {
		// No post-processing — chunk manager doesn't implement the
		// processor interface (memory / jsonl). Records are already
		// durable on the local manager; no further work needed.
		return
	}
	name := fmt.Sprintf("post-seal:%s:%s", vaultID, chunkID)
	wrappedFn := func(ctx context.Context, id chunk.ChunkID) error {
		if err := processor.PostSealProcess(ctx, id); err != nil {
			return err
		}
		// Notify: compression/indexing done, chunk meta changed again
		// (DiskBytes, etc.). Carry the fresh meta so clients can patch
		// their cache without a refetch.
		if meta, err := cm.Meta(id); err == nil {
			o.EmitChunkSealed(vaultID, meta)
		} else {
			o.NotifyChunkChange()
		}
		return nil
	}
	if err := o.scheduler.RunOnce(name, wrappedFn, context.Background(), chunkID); err != nil {
		o.logger.Warn("failed to schedule post-seal", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Post-seal pipeline for chunk %s", chunkID))
}
