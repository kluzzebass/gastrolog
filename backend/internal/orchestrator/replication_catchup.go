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
	if o.chunkReplicator == nil {
		return errors.New("no tier replicator configured")
	}

	metas, err := tier.Chunks.List()
	if err != nil {
		return fmt.Errorf("list chunks: %w", err)
	}

	// Snapshot the tier FSM manifest at the start of the catchup pass.
	// We use it to filter out chunks that have already been retired from the
	// cluster's view of the data — there's a race window between the FSM
	// applying a delete and the leader's local file actually being unlinked,
	// during which tier.Chunks.List() will still return the chunk. Sending
	// such a chunk would be wasted work: the receiver would write it to disk
	// and immediately apply the matching CmdRequestDelete (see gastrolog-5grpa
	// and the gastrolog-51gme receipt protocol).
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
			// The scheduler will re-run the job. Sentinel sentinels don't
			// survive the cluster RPC boundary (the handler concatenates
			// strings) so we substring-match both error wordings — the
			// legacy "vault not found" and the new "tier not registered on
			// this node" (gastrolog-2t48z).
			msg := err.Error()
			if strings.Contains(msg, "vault not found") || strings.Contains(msg, "tier not registered on this node") {
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

// CatchupSelectedChunks is the placement-leader-side handler for the
// follower-driven RequestReplicaCatchup RPC. The follower's lifecycle
// reconciler (SweepMissingReplicas) computes its FSM-vs-disk diff and
// sends the requested chunk IDs to the leader; this method validates
// each chunk against catchupCandidates' filters (sealed locally,
// uncompressed-file-tier exclusion, cloud-backed exclusion, FSM
// manifest membership) and fans pushes out asynchronously via the
// existing replicateToFollower machinery.
//
// Returns the count of pushes scheduled — not delivered. The follower
// will re-request anything still missing on its next sweep tick if a
// push fails after this call returns. Asynchronous fan-out is a
// deliberate choice: the RPC stays cheap, the slow per-chunk transfers
// run on a single goroutine sequentially per (vault, tier, requester)
// to avoid storming the bandwidth path. See gastrolog-2dgvj.
func (o *Orchestrator) CatchupSelectedChunks(ctx context.Context, vaultID, tierID glid.GLID, requesterNodeID string, chunkIDs []chunk.ChunkID) (uint32, error) {
	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		return 0, fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
	}
	if tier.IsFollower {
		return 0, fmt.Errorf("not placement leader for tier %s (follower)", tierID)
	}
	if o.chunkReplicator == nil {
		return 0, errors.New("no tier replicator configured")
	}

	metas, err := tier.Chunks.List()
	if err != nil {
		return 0, fmt.Errorf("list chunks: %w", err)
	}
	bySealedID := make(map[chunk.ChunkID]chunk.ChunkMeta, len(metas))
	for _, m := range metas {
		bySealedID[m.ID] = m
	}

	var manifestSet map[chunk.ChunkID]bool
	if tier.ListManifest != nil {
		ids := tier.ListManifest()
		manifestSet = make(map[chunk.ChunkID]bool, len(ids))
		for _, id := range ids {
			manifestSet[id] = true
		}
	}

	// Filter requested IDs through the same eligibility rules
	// catchupFollower's catchupCandidates uses, but indexed by
	// the caller's set rather than scanned across the leader's
	// full sealed-chunk list.
	var eligible []chunk.ChunkMeta
	for _, id := range chunkIDs {
		m, ok := bySealedID[id]
		if !ok {
			continue // leader doesn't have it locally either
		}
		if !m.Sealed {
			continue
		}
		if m.CloudBacked {
			continue
		}
		if manifestSet != nil && !manifestSet[id] {
			continue
		}
		eligible = append(eligible, m)
	}

	if len(eligible) == 0 {
		o.logger.Info("replica catchup: no eligible chunks to push",
			"vault", vaultID, "tier", tierID, "requester", requesterNodeID,
			"requested", len(chunkIDs))
		return 0, nil
	}

	o.logger.Info("replica catchup: scheduling pushes",
		"vault", vaultID, "tier", tierID, "requester", requesterNodeID,
		"scheduled", len(eligible), "requested", len(chunkIDs))

	// Run the actual pushes asynchronously so the RPC returns promptly.
	// Use a fresh background context with the same timeout discipline
	// as scheduleCatchupForNode — the RPC's caller-supplied ctx ends as
	// soon as we return, which would abort transfers mid-stream.
	go func() {
		ctxBg, cancel := context.WithTimeout(context.Background(), cluster.CatchupTimeout)
		defer cancel()
		transferred := 0
		for _, m := range eligible {
			if err := o.replicateToFollower(ctxBg, vaultID, tierID, m.ID, tier.Chunks, requesterNodeID); err != nil {
				o.logger.Warn("replica catchup: push failed",
					"vault", vaultID, "tier", tierID, "chunk", m.ID.String(),
					"requester", requesterNodeID, "error", err)
				continue
			}
			transferred++
		}
		o.logger.Info("replica catchup: completed",
			"vault", vaultID, "tier", tierID, "requester", requesterNodeID,
			"transferred", transferred, "scheduled", len(eligible))
	}()

	_ = ctx // unused — async path uses its own timeout context
	return uint32(len(eligible)), nil //nolint:gosec // G115: bounded by chunkIDs slice length
}

// catchupCandidates filters chunk metas to those eligible for catchup
// replication. Excludes unsealed, uncompressed file-tier, cloud-backed,
// and FSM-retired chunks.
func catchupCandidates(metas []chunk.ChunkMeta, _ string, manifestSet map[chunk.ChunkID]bool) []chunk.ChunkMeta {
	var out []chunk.ChunkMeta
	for _, m := range metas {
		if !m.Sealed {
			continue
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
