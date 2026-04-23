package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
)

// ScheduleCatchup schedules catchup replication for newly added followers of
// a tier within the given vault. Must be called on the node that holds the
// tier leader replica — no-op if this node is a follower or does not host
// the tier. The caller owns the (vaultID, tierID) pair; the orchestrator
// does not reverse-lookup by tierID alone.
func (o *Orchestrator) ScheduleCatchup(vaultID, tierID glid.GLID, followerNodeIDs []string) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	var found *TierInstance
	if vault != nil {
		for _, t := range vault.Tiers {
			if t.TierID == tierID {
				found = t
				break
			}
		}
	}
	o.mu.RUnlock()
	if found == nil || found.IsFollower {
		return
	}
	o.scheduleCatchup(vaultID, tierID, followerNodeIDs)
}

// scheduleCatchup schedules background jobs to replicate existing sealed chunks
// from the leader to newly added follower nodes.
func (o *Orchestrator) scheduleCatchup(vaultID, tierID glid.GLID, newFollowers []string) {
	for _, nodeID := range newFollowers {
		o.scheduleCatchupForNode(vaultID, tierID, nodeID, 0)
	}
}

const maxCatchupRetries = 3

func (o *Orchestrator) scheduleCatchupForNode(vaultID, tierID glid.GLID, nodeID string, attempt int) {
	name := "replication-catchup:" + vaultID.String() + ":" + tierID.String() + ":" + nodeID
	if attempt > 0 {
		name += fmt.Sprintf(":retry-%d", attempt)
	}
	if err := o.scheduler.RunOnce(name, func() {
		// On retries, wait for the recovering node to finish building
		// its tiers. The tier appears within a few seconds as the
		// dispatch processes Raft notifications after ApplyConfig.
		if attempt > 0 {
			<-time.After(5 * time.Second)
		}
		ctx, cancel := context.WithTimeout(context.Background(), cluster.CatchupTimeout)
		defer cancel()
		if err := o.catchupFollower(ctx, vaultID, tierID, nodeID); err != nil {
			if attempt < maxCatchupRetries && strings.Contains(err.Error(), "not ready") {
				o.logger.Info("catchup: follower not ready, will retry",
					"vault", vaultID, "tier", tierID, "node", nodeID,
					"attempt", attempt+1)
				o.scheduleCatchupForNode(vaultID, tierID, nodeID, attempt+1)
			} else {
				o.logger.Warn("catchup failed", "vault", vaultID, "tier", tierID, "node", nodeID, "error", err)
			}
		}
	}); err != nil {
		o.logger.Warn("failed to schedule replication catchup", "name", name, "error", err)
	}
	o.scheduler.Describe(name, "Replicate sealed chunks to follower "+nodeID[:8])
}

// catchupFollower copies all sealed chunks from the leader's tier to a
// follower node. Each chunk's records are streamed via TransferRecords,
// producing an identical sealed chunk on the follower.
func (o *Orchestrator) catchupFollower(ctx context.Context, vaultID, tierID glid.GLID, nodeID string) error {
	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		return fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
	}
	if tier.IsFollower {
		return nil // only leader initiates catchup
	}
	if o.tierReplicator == nil {
		return errors.New("no tier replicator configured")
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

	sealed := catchupCandidates(metas, tier.Type, manifestSet)

	if len(sealed) == 0 {
		o.logger.Debug("replication catchup: no sealed chunks to copy",
			"vault", vaultID, "tier", tierID, "follower", nodeID)
		return nil
	}

	o.logger.Info("replication catchup: starting",
		"vault", vaultID, "tier", tierID, "follower", nodeID, "chunks", len(sealed))

	transferred := 0
	for _, meta := range sealed {
		if err := o.replicateToFollower(ctx, vaultID, tierID, meta.ID, tier.Chunks, nodeID); err != nil {
			// If the follower rejected because its tier isn't built yet
			// (recovering node still in startup), return a retryable error.
			// The scheduler will re-run the job.
			if strings.Contains(err.Error(), "vault not found") {
				return fmt.Errorf("follower %s not ready for tier %s (still building): %w", nodeID, tierID, err)
			}
			o.logger.Warn("replication catchup: transfer failed",
				"chunk", meta.ID.String(), "follower", nodeID, "error", err)
			continue
		}
		transferred++
		o.logger.Debug("replication catchup: chunk transferred",
			"vault", vaultID, "tier", tierID, "chunk", meta.ID.String(), "follower", nodeID,
			"records", meta.RecordCount)
	}

	o.logger.Info("replication catchup: completed",
		"vault", vaultID, "tier", tierID, "follower", nodeID,
		"transferred", transferred, "total", len(sealed))
	return nil
}

// catchupCandidates filters chunk metas to those eligible for catchup
// replication. Excludes unsealed, uncompressed file-tier, cloud-backed,
// and FSM-retired chunks.
func catchupCandidates(metas []chunk.ChunkMeta, tierType string, manifestSet map[chunk.ChunkID]bool) []chunk.ChunkMeta {
	var out []chunk.ChunkMeta
	isFileTier := tierType == "file"
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		if isFileTier && !m.Compressed {
			continue // post-seal pipeline will replicate after compression
		}
		if m.CloudBacked {
			continue // cloud-backed chunks replicate via FSM (RegisterCloudChunk), not record streaming
		}
		if manifestSet != nil && !manifestSet[m.ID] {
			continue // FSM has retired this chunk — don't ship orphans
		}
		out = append(out, m)
	}
	return out
}
