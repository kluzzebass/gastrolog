package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"

	"github.com/google/uuid"
)

// ScheduleCatchupForTier finds the vault containing the given tier and
// schedules catchup for the specified followers.
func (o *Orchestrator) ScheduleCatchupForTier(tierID uuid.UUID, followerNodeIDs []string) {
	o.mu.RLock()
	for vaultID, vault := range o.vaults {
		for _, t := range vault.Tiers {
			if t.TierID == tierID && !t.IsFollower {
				o.mu.RUnlock()
				o.scheduleCatchup(vaultID, tierID, followerNodeIDs)
				return
			}
		}
	}
	o.mu.RUnlock()
}

// scheduleCatchup schedules background jobs to replicate existing sealed chunks
// from the leader to newly added follower nodes.
func (o *Orchestrator) scheduleCatchup(vaultID, tierID uuid.UUID, newFollowers []string) {
	for _, nodeID := range newFollowers {
		name := "replication-catchup:" + vaultID.String() + ":" + tierID.String() + ":" + nodeID
		node := nodeID // capture for closure
		if err := o.scheduler.RunOnce(name, func() {
			// Created inside the closure so the timeout starts when the job
			// executes, not when it's scheduled.
			ctx, cancel := context.WithTimeout(context.Background(), cluster.CatchupTimeout)
			defer cancel()
			if err := o.catchupFollower(ctx, vaultID, tierID, node); err != nil {
				o.logger.Warn("catchup failed", "vault", vaultID, "tier", tierID, "node", node, "error", err)
			}
		}); err != nil {
			o.logger.Warn("failed to schedule replication catchup", "name", name, "error", err)
		}
		o.scheduler.Describe(name, "Replicate sealed chunks to follower "+nodeID[:8])
	}
}

// catchupFollower copies all sealed chunks from the leader's tier to a
// follower node. Each chunk's records are streamed via TransferRecords,
// producing an identical sealed chunk on the follower.
func (o *Orchestrator) catchupFollower(ctx context.Context, vaultID, tierID uuid.UUID, nodeID string) error {
	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		return fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
	}
	if tier.IsFollower {
		return nil // only leader initiates catchup
	}
	if o.transferrer == nil {
		return errors.New("no remote transferrer configured")
	}

	metas, err := tier.Chunks.List()
	if err != nil {
		return fmt.Errorf("list chunks: %w", err)
	}

	// Snapshot the tier Raft FSM manifest at the start of the catchup pass.
	// We use it to filter out chunks that have already been retired from the
	// cluster's view of the data — there's a race window between the FSM
	// applying a DeleteChunk and the leader's local file actually being
	// unlinked, during which tier.Chunks.List() will still return the chunk.
	// Sending such a chunk would be wasted work: the receiver would write it
	// to disk and then immediately delete it via reconcileFollower, because
	// reconcile uses the same FSM manifest as ground truth. See gastrolog-5grpa.
	var manifestSet map[chunk.ChunkID]bool
	if tier.ListManifest != nil {
		ids := tier.ListManifest()
		manifestSet = make(map[chunk.ChunkID]bool, len(ids))
		for _, id := range ids {
			manifestSet[id] = true
		}
	}

	// Only replicate chunks where post-seal is complete. For file tiers,
	// this means compressed — uncompressed sealed chunks are still in the
	// post-seal pipeline (compress → index → replicate), which will handle
	// replication itself. For memory tiers, all sealed chunks are ready.
	var sealed []chunk.ChunkMeta
	isFileTier := tier.Type == "file"
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		if isFileTier && !m.Compressed {
			continue // post-seal pipeline will replicate after compression
		}
		if manifestSet != nil && !manifestSet[m.ID] {
			continue // FSM has retired this chunk — don't ship orphans
		}
		sealed = append(sealed, m)
	}

	if len(sealed) == 0 {
		o.logger.Info("replication catchup: no sealed chunks to copy",
			"vault", vaultID, "tier", tierID, "follower", nodeID)
		return nil
	}

	o.logger.Info("replication catchup: starting",
		"vault", vaultID, "tier", tierID, "follower", nodeID, "chunks", len(sealed))

	for _, meta := range sealed {
		if err := o.replicateToFollower(ctx, vaultID, tierID, meta.ID, tier.Chunks, nodeID); err != nil {
			o.logger.Warn("replication catchup: transfer failed",
				"chunk", meta.ID.String(), "follower", nodeID, "error", err)
			continue
		}
		o.logger.Info("replication catchup: chunk transferred",
			"vault", vaultID, "tier", tierID, "chunk", meta.ID.String(), "follower", nodeID,
			"records", meta.RecordCount)
	}

	o.logger.Info("replication catchup: completed",
		"vault", vaultID, "tier", tierID, "follower", nodeID, "chunks", len(sealed))
	return nil
}
