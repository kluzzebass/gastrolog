package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"

	"github.com/google/uuid"
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
	_, err := o.ingest(rec)
	return err
}

// ingest is the internal ingest implementation, called by processMessage.
// Extracted from Ingest to allow both direct and channel-based ingestion.
//
// Returns pendingAcks bundling the sync work an ack-gated record triggers:
// local tier replication to followers, plus cross-node forwarding of
// records matched to remote vaults. Both task kinds must complete before
// the ack is delivered to the ingester. For non-ack-gated records,
// pendingAcks is always empty — remote forwarding runs fire-and-forget
// through forwardRemote(). See gastrolog-27zvt.
func (o *Orchestrator) ingest(rec chunk.Record) (*pendingAcks, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	o.routeStats.Ingested.Add(1)

	if len(o.vaults) == 0 && o.forwarder == nil {
		o.routeStats.Dropped.Add(1)
		return nil, ErrNoChunkManagers
	}

	if o.filterSet == nil {
		o.routeStats.Dropped.Add(1)
		return nil, nil // No routes configured — drop the record.
	}

	matches := o.filterSet.MatchWithNode(rec.Attrs)
	if len(matches) == 0 {
		o.routeStats.Dropped.Add(1)
		return nil, nil
	}

	// Write records first, then update stats only on success.
	routed := false
	var pa *pendingAcks
	for _, t := range matches {
		if t.NodeID != "" {
			// Remote vault match. Fire-and-forget for non-ack-gated;
			// accumulate a forwardTask for ack-gated records so the ack
			// waits for the forward channel to accept the record.
			if rec.WaitForReplica {
				pa = pa.addForward(forwardTask{nodeID: t.NodeID, vaultID: t.VaultID})
			} else {
				o.forwardRemote(t, rec)
			}
			vs := o.getOrCreateVaultRouteStats(t.VaultID)
			vs.Matched.Add(1)
			vs.Forwarded.Add(1)
			if t.RouteID != uuid.Nil {
				rs := o.getOrCreatePerRouteStats(t.RouteID)
				rs.Matched.Add(1)
				rs.Forwarded.Add(1)
			}
			routed = true
			continue
		}
		task, err := o.appendLocal(t.VaultID, rec)
		if err != nil {
			if errors.Is(err, ErrVaultDisabled) {
				continue // Skip disabled vaults during ingestion.
			}
			return pa, err
		}
		if task != nil {
			pa = pa.addReplication(*task)
		}
		vs := o.getOrCreateVaultRouteStats(t.VaultID)
		vs.Matched.Add(1)
		if t.RouteID != uuid.Nil {
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

// pendingAcks bundles the sync work that an ack-gated record triggers —
// local tier replication to followers and cross-node forwarding of
// records matched to remote vaults. Both must complete before the ack
// is delivered to the ingester.
//
// Nil receiver is treated as empty; addReplication/addForward lazy-init
// so callers don't have to check before appending.
type pendingAcks struct {
	replication []replicationTask
	forwards    []forwardTask
}

func (p *pendingAcks) addReplication(t replicationTask) *pendingAcks {
	if p == nil {
		p = &pendingAcks{}
	}
	p.replication = append(p.replication, t)
	return p
}

func (p *pendingAcks) addForward(t forwardTask) *pendingAcks {
	if p == nil {
		p = &pendingAcks{}
	}
	p.forwards = append(p.forwards, t)
	return p
}

// isEmpty reports whether there is any sync work to wait on before acking.
func (p *pendingAcks) isEmpty() bool {
	return p == nil || (len(p.replication) == 0 && len(p.forwards) == 0)
}

// forwardTask is a pending cross-node forward for an ack-gated record
// that matched a filter targeting a vault on another node. Processed
// synchronously by ackAfterReplication via RecordForwarder.ForwardSync
// so the ack-gated durability guarantee extends to cross-node routes.
type forwardTask struct {
	nodeID  string
	vaultID uuid.UUID
}

// getOrCreateVaultRouteStats returns the per-vault route stats, creating if needed.
func (o *Orchestrator) getOrCreateVaultRouteStats(vaultID uuid.UUID) *VaultRouteStats {
	if v, ok := o.vaultRouteStats.Load(vaultID); ok {
		return v.(*VaultRouteStats)
	}
	v, _ := o.vaultRouteStats.LoadOrStore(vaultID, &VaultRouteStats{})
	return v.(*VaultRouteStats)
}

// getOrCreatePerRouteStats returns the per-route stats, creating if needed.
func (o *Orchestrator) getOrCreatePerRouteStats(routeID uuid.UUID) *PerRouteStats {
	if v, ok := o.perRouteStats.Load(routeID); ok {
		return v.(*PerRouteStats)
	}
	v, _ := o.perRouteStats.LoadOrStore(routeID, &PerRouteStats{})
	return v.(*PerRouteStats)
}

// appendLocal appends a record to a local vault.
// Returns a replicationTask when the record needs sync forwarding (ack-gated).
// Remote fire-and-forget forwards are dispatched after the lock is released.
func (o *Orchestrator) appendLocal(vaultID uuid.UUID, rec chunk.Record) (*replicationTask, error) {
	_, _, task, remotes, err := o.appendRecord(vaultID, rec)
	if err != nil {
		o.logger.Error("append to vault failed", "vault", vaultID, "error", err)
	}
	// Remote forwards happen outside the orchestrator lock.
	o.fireAndForgetRemote(remotes, rec)
	return task, err
}

// forwardRemote ships a record to the node that owns the target vault.
// Uses a tight timeout to prevent a slow peer from blocking the orchestrator
// lock (held by the caller during ingestion).
func (o *Orchestrator) forwardRemote(t MatchResult, rec chunk.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), cluster.ForwardingTimeout)
	defer cancel()
	if err := o.forwarder.Forward(ctx, t.NodeID, t.VaultID, []chunk.Record{rec}); err != nil {
		o.logger.Warn("forward record failed", "node", t.NodeID, "vault", t.VaultID, "error", err)
	}
}

// postSealWork schedules the post-seal pipeline for a newly sealed chunk.
// Safe to call from any context (cron rotation, background sweep, etc.) —
// acquires the orchestrator lock internally.
func (o *Orchestrator) postSealWork(vaultID uuid.UUID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	o.schedulePostSeal(vaultID, cm, chunkID)
	// Notify WatchChunks subscribers: the chunk's sealed flag has changed.
	o.NotifyChunkChange()
}

// schedulePostSeal schedules the unified post-seal pipeline (compress → index → upload).
// If the chunk manager implements ChunkPostSealProcessor, the entire pipeline runs
// as one sequential job. Otherwise falls back to compress-only for non-file managers.
// After the pipeline completes, sealed-chunk replication is triggered for leader tiers.
func (o *Orchestrator) schedulePostSeal(vaultID uuid.UUID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	// Resolve tier info for post-pipeline replication.
	tierID, followerTargets := o.tierReplicationInfo(vaultID, cm)

	processor, ok := cm.(chunk.ChunkPostSealProcessor)
	if ok {
		name := fmt.Sprintf("post-seal:%s:%s", vaultID, chunkID)
		wrappedFn := func(ctx context.Context, id chunk.ChunkID) error {
			if err := processor.PostSealProcess(ctx, id); err != nil {
				return err
			}
			// Notify: compression/indexing done, metadata changed again.
			o.NotifyChunkChange()
			// Schedule replication as a separate job — never blocks the
			// post-seal scheduler slot.
			o.scheduleReplication(vaultID, tierID, id, followerTargets)
			return nil
		}
		if err := o.scheduler.RunOnce(name, wrappedFn, context.Background(), chunkID); err != nil {
			o.logger.Warn("failed to schedule post-seal", "name", name, "error", err)
		}
		o.scheduler.Describe(name, fmt.Sprintf("Post-seal pipeline for chunk %s", chunkID))
		return
	}

	// Fallback for non-file managers (e.g. memory) — compress only, then replicate.
	compressor, ok := cm.(chunk.ChunkCompressor)
	if ok {
		name := fmt.Sprintf("compress:%s:%s", vaultID, chunkID)
		wrappedFn := func(id chunk.ChunkID) error {
			if err := compressor.CompressChunk(id); err != nil {
				return err
			}
			o.NotifyChunkChange()
			o.scheduleReplication(vaultID, tierID, id, followerTargets)
			return nil
		}
		if err := o.scheduler.RunOnce(name, wrappedFn, chunkID); err != nil {
			o.logger.Warn("failed to schedule compression", "name", name, "error", err)
		}
		return
	}

	// No post-processing — schedule replication directly.
	o.scheduleReplication(vaultID, tierID, chunkID, followerTargets)
}

// tierReplicationInfo returns the tier ID and follower targets for the tier
// that owns the given ChunkManager. Returns zero values if not found or if the
// tier is a follower (followers don't replicate further).
func (o *Orchestrator) tierReplicationInfo(vaultID uuid.UUID, cm chunk.ChunkManager) (uuid.UUID, []system.ReplicationTarget) {
	vault := o.vaults[vaultID]
	if vault == nil {
		return uuid.UUID{}, nil
	}
	for _, tier := range vault.Tiers {
		if tier.Chunks == cm && tier.ShouldForwardToFollowers() {
			return tier.TierID, tier.FollowerTargets
		}
	}
	return uuid.UUID{}, nil
}
