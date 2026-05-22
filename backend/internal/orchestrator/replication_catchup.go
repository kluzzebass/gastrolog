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

// ScheduleCatchup schedules catchup replication pushes for the given
// peer node IDs. Called when peers join a placement and the local
// node wants to proactively push sealed chunks rather than wait for
// the peers' periodic SweepMissingReplicas tick. No-op if this node
// has no vault instance (nothing to push). Under fan-out every
// Receiver can drive a push; the legacy "only the leader can drive
// catchup" gate is gone (gastrolog-3vhu4).
func (o *Orchestrator) ScheduleCatchup(vaultID glid.GLID, peerNodeIDs []string) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	var found *VaultInstance
	if vault != nil && vault.Instance != nil {
		found = vault.Instance
	}
	o.mu.RUnlock()
	if found == nil {
		return
	}
	o.scheduleCatchup(vaultID, peerNodeIDs)
}

// scheduleCatchup schedules background jobs to replicate existing sealed chunks
// from the leader to newly added follower nodes.
func (o *Orchestrator) scheduleCatchup(vaultID glid.GLID, newFollowers []string) {
	for _, nodeID := range newFollowers {
		o.scheduleCatchupForNode(vaultID, nodeID, 0)
	}
}

const maxCatchupRetries = 3

func (o *Orchestrator) scheduleCatchupForNode(vaultID glid.GLID, nodeID string, attempt int) {
	name := "replication-catchup:" + vaultID.String() + ":" + nodeID
	if attempt > 0 {
		name += fmt.Sprintf(":retry-%d", attempt)
	}
	if err := o.scheduler.RunOnce(name, func() {
		// On retries, wait for the recovering node to finish building
		// its vaults. The instance appears within a few seconds as the
		// dispatch processes Raft notifications after ApplyConfig.
		if attempt > 0 {
			<-time.After(5 * time.Second)
		}
		ctx, cancel := context.WithTimeout(context.Background(), cluster.CatchupTimeout)
		defer cancel()
		if err := o.catchupFollower(ctx, vaultID, nodeID); err != nil {
			if attempt < maxCatchupRetries && strings.Contains(err.Error(), "not ready") {
				o.replicationLogger.Info("catchup: follower not ready, will retry",
					"vault", vaultID, "node", nodeID,
					"attempt", attempt+1)
				o.scheduleCatchupForNode(vaultID, nodeID, attempt+1)
			} else {
				o.replicationLogger.Warn("catchup failed", "vault", vaultID, "node", nodeID, "error", err)
			}
		}
	}); err != nil {
		o.replicationLogger.Warn("failed to schedule replication catchup", "name", name, "error", err)
	}
	o.scheduler.Describe(name, "Replicate sealed chunks to follower "+nodeID[:8])
}

// catchupFollower copies all sealed chunks from the leader's vault instance
// to a follower node. Each chunk's records are streamed via TransferRecords,
// producing an identical sealed chunk on the follower.
func (o *Orchestrator) catchupFollower(ctx context.Context, vaultID glid.GLID, nodeID string) error {
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst == nil {
		return fmt.Errorf("vault %s not found", vaultID)
	}
	if o.chunkReplicator == nil {
		return errors.New("no chunk replicator configured")
	}

	metas, err := vaultInst.Chunks.List()
	if err != nil {
		return fmt.Errorf("list chunks: %w", err)
	}

	// Snapshot the vault-ctl FSM manifest at the start of the catchup pass.
	// We use it to filter out chunks that have already been retired from the
	// cluster's view of the data — there's a race window between the FSM
	// applying a delete and the leader's local file actually being unlinked,
	// during which instance.Chunks.List() will still return the chunk. Sending
	// such a chunk would be wasted work: the receiver would write it to disk
	// and immediately apply the matching CmdRequestDelete (see gastrolog-5grpa
	// and the gastrolog-51gme receipt protocol).
	var manifestSet map[chunk.ChunkID]bool
	if vaultInst.ListManifest != nil {
		ids := vaultInst.ListManifest()
		manifestSet = make(map[chunk.ChunkID]bool, len(ids))
		for _, id := range ids {
			manifestSet[id] = true
		}
	}

	// Phase 3 (gastrolog-1huz5): overlay through FSM so catchupCandidates'
	// .Sealed gate excludes Sealing chunks (GLCB not yet committed).
	if vaultInst.OverlayFromFSM != nil {
		for i := range metas {
			metas[i] = vaultInst.OverlayFromFSM(metas[i])
		}
	}
	sealed := catchupCandidates(metas, vaultInst.Type, manifestSet)

	if len(sealed) == 0 {
		o.replicationLogger.Debug("replication catchup: no sealed chunks to copy",
			"vault", vaultID, "follower", nodeID)
		return nil
	}

	o.replicationLogger.Info("replication catchup: starting",
		"vault", vaultID, "follower", nodeID, "chunks", len(sealed))

	transferred := 0
	for _, meta := range sealed {
		if err := o.replicateToFollower(ctx, vaultID, meta.ID, vaultInst.Chunks, nodeID); err != nil {
			// If the follower rejected because its vault isn't built yet
			// (recovering node still in startup), return a retryable error.
			// The scheduler will re-run the job. Sentinel errors don't
			// survive the cluster RPC boundary (the handler concatenates
			// strings) so we substring-match both error wordings — the
			// legacy "vault not found" and the new "instance not registered
			// on this node" (gastrolog-2t48z).
			msg := err.Error()
			if strings.Contains(msg, "vault not found") || strings.Contains(msg, "instance not registered on this node") {
				return fmt.Errorf("follower %s not ready for vault %s (still building): %w", nodeID, vaultID, err)
			}
			o.replicationLogger.Warn("replication catchup: transfer failed",
				"chunk", meta.ID.String(), "follower", nodeID, "error", err)
			continue
		}
		transferred++
		o.replicationLogger.Debug("replication catchup: chunk transferred",
			"vault", vaultID, "chunk", meta.ID.String(), "follower", nodeID,
			"records", meta.RecordCount)
	}

	o.replicationLogger.Info("replication catchup: completed",
		"vault", vaultID, "follower", nodeID,
		"transferred", transferred, "total", len(sealed))
	return nil
}

// CatchupSelectedChunks is the receiver-side handler for the
// RequestReplicaCatchup RPC. A peer's lifecycle reconciler
// (SweepMissingReplicas) computes its FSM-vs-disk diff and sends the
// requested chunk IDs to any peer that might have them; this method
// validates each chunk against catchupCandidates' filters (sealed
// locally, cloud-backed exclusion, FSM manifest membership) and fans
// pushes out asynchronously via the existing replicateToFollower
// machinery.
//
// Returns the count of pushes scheduled — not delivered. The requester
// will re-request anything still missing on its next sweep tick if a
// push fails after this call returns. Asynchronous fan-out is a
// deliberate choice: the RPC stays cheap, the slow per-chunk transfers
// run on a single goroutine sequentially per (vault, requester) to
// avoid storming the bandwidth path.
//
// Symmetric peer-to-peer (gastrolog-19241): both followers and leaders
// can be requesters AND responders. The receiver doesn't need to be the
// placement leader — it just needs to have the chunks locally. This
// enables a newly-elected leader to backfill historical chunks from
// followers that still have them, instead of waiting for the stale-fsm
// sweep to declare the chunks unrecoverable. See gastrolog-2dgvj for
// the original (follower→leader) design.
func (o *Orchestrator) CatchupSelectedChunks(ctx context.Context, vaultID glid.GLID, requesterNodeID string, chunkIDs []chunk.ChunkID) (uint32, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil || vault.Instance == nil {
		return 0, fmt.Errorf("vault %s not found", vaultID)
	}
	vaultInst := vault.Instance
	if o.chunkReplicator == nil {
		return 0, errors.New("no chunk replicator configured")
	}

	metas, err := vaultInst.Chunks.List()
	if err != nil {
		return 0, fmt.Errorf("list chunks: %w", err)
	}
	bySealedID := make(map[chunk.ChunkID]chunk.ChunkMeta, len(metas))
	for _, m := range metas {
		bySealedID[m.ID] = m
	}

	var manifestSet map[chunk.ChunkID]bool
	if vaultInst.ListManifest != nil {
		ids := vaultInst.ListManifest()
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
		// Phase 3 (gastrolog-1huz5): catchup ships sealed-form GLCBs
		// only. Sealing chunks have no GLCB yet — overlay through the
		// FSM so we don't queue a push for a chunk that's still in
		// assembly.
		if vaultInst.OverlayFromFSM != nil {
			m = vaultInst.OverlayFromFSM(m)
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
		o.replicationLogger.Info("replica catchup: no eligible chunks to push",
			"vault", vaultID, "requester", requesterNodeID,
			"requested", len(chunkIDs))
		return 0, nil
	}

	o.replicationLogger.Info("replica catchup: scheduling pushes",
		"vault", vaultID, "requester", requesterNodeID,
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
			if err := o.replicateToFollower(ctxBg, vaultID, m.ID, vaultInst.Chunks, requesterNodeID); err != nil {
				o.replicationLogger.Warn("replica catchup: push failed",
					"vault", vaultID, "chunk", m.ID.String(),
					"requester", requesterNodeID, "error", err)
				continue
			}
			transferred++
		}
		o.replicationLogger.Info("replica catchup: completed",
			"vault", vaultID, "requester", requesterNodeID,
			"transferred", transferred, "scheduled", len(eligible))
	}()

	_ = ctx // unused — async path uses its own timeout context
	return uint32(len(eligible)), nil //nolint:gosec // G115: bounded by chunkIDs slice length
}

// catchupCandidates filters chunk metas to those eligible for catchup
// replication. Excludes unsealed, cloud-backed, and FSM-retired chunks.
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
