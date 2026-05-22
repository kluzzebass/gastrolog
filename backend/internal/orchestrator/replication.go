package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"

	"golang.org/x/sync/errgroup"
)

// SealActiveChunk seals the active chunk for a vault on a **follower**
// node, as the local effect of a leader-originated SealVault replication
// command. Use SealActive on the leader-triggered path.
//
// Role: follower-side. Caller is typically the ChunkReplicator handler that
// receives the seal command from the leader. Validates expectedChunkID to
// avoid sealing the wrong chunk if rotation raced the seal command.
//
// Readiness: no Vault.ReadinessErr gate — this call executes a replicated
// command that the leader already authorized. The follower's own FSM
// manifest may lag, but the physical seal (flush + close file) is local
// and safe regardless.
//
// Do not merge with SealActive: the two paths run on different nodes with
// different invariants. SealActive (leader) fans out replication; this
// function is the target of that fan-out on followers.
func (o *Orchestrator) SealActiveChunk(vaultID glid.GLID, expectedChunkID chunk.ChunkID) error {
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst == nil {
		return fmt.Errorf("%w: vault %s", ErrInstanceNotLocal, vaultID)
	}
	active := vaultInst.Chunks.Active()
	if active == nil {
		return nil // nothing to seal
	}
	if active.ID != expectedChunkID {
		o.replicationLogger.Debug("replication: seal skipped — chunk already rotated",
			"vault", vaultID,
			"expected", expectedChunkID.String(), "active", active.ID.String())
		return nil
	}
	chunkID := active.ID
	if err := vaultInst.Chunks.Seal(); err != nil {
		return err
	}
	o.postSealWork(vaultID, vaultInst.Chunks, chunkID)
	return nil
}

// ackAfterReplication does sync forwarding to followers and sync
// cross-node forwarding for ack-gated records, then sends the ack.
// Runs in a goroutine — doesn't block the writeLoop.
//
// All vault follower AppendRecords and all cross-node ForwardSync calls run
// concurrently under one deadline (cluster.ReplicationTimeout). The first
// error wins and is sent to the ack channel; errgroup cancels the shared
// context so other RPCs stop promptly.
//
// Cross-node forwarding uses RecordForwarder.ForwardSync for records that
// matched filters targeting vaults on other nodes. See gastrolog-27zvt:
// before that fix, ack-gated remote routes were fire-and-forget and could
// be silently dropped on a full forward buffer.
func (o *Orchestrator) ackAfterReplication(ack chan<- error, pa *pendingAcks, rec chunk.Record) {
	if pa == nil {
		ack <- nil
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cluster.ReplicationTimeout)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// Fan-out W-of-N tasks (gastrolog-nd6sz). Under mqxo4 the legacy
	// forwardTask / pa.forwards path is gone — every cross-node record
	// dispatch flows through fanOut, regardless of whether this node
	// has a local vault instance. Each task waits in its own errgroup
	// goroutine; runFanOut internally launches the per-peer goroutines
	// + the waitWOfN coordinator. A failure (ErrWOfNUnreachable)
	// propagates to the ack channel like any other replication error.
	for _, t := range pa.fanOut {
		g.Go(func() error {
			if err := o.runFanOut(ctx, &t, rec); err != nil {
				return fmt.Errorf("ack-gated fan-out vault=%s chunk=%s: %w", t.vaultID, t.chunkID, err)
			}
			return nil
		})
	}

	ack <- g.Wait()
}

// replicateToFollower streams a single sealed chunk to one follower node.
// Validates that the chunk is readable before opening the network stream —
// corrupted chunks fail fast without touching the wire.
func (o *Orchestrator) replicateToFollower(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, cm chunk.ChunkManager, nodeID string) error {
	if o.chunkReplicator == nil {
		return errors.New("replicateToFollower: chunk replicator not configured")
	}
	// Pre-flight: open and read the first record to confirm the chunk is intact.
	// Corrupted compressed data fails here instantly — no network round-trip.
	probe, err := cm.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	_, _, probeErr := probe.Next()
	_ = probe.Close()
	if probeErr != nil && !errors.Is(probeErr, chunk.ErrNoMoreRecords) {
		return fmt.Errorf("chunk unreadable: %w", probeErr)
	}

	// Chunk is readable — open a fresh cursor for the actual transfer.
	// The cursor streams records on demand; ImportSealedChunk consumes it
	// via a RecordIterator, so nothing proportional to chunk size lands on
	// the leader's heap during the push. See gastrolog-4yvhh.
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	// Final tombstone check right before sending: retention may have
	// deleted this chunk while we were validating readability. Without
	// the recheck, a late ImportSealed would still land on the follower
	// after the follower has already processed the delete via vault-ctl
	// Raft, and the follower's post-import cleanup only catches the
	// case where the tombstone is on its own FSM. This leader-side
	// recheck short-circuits the RPC entirely when the leader already
	// knows the chunk is gone. See gastrolog-11rzz.
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst != nil && vaultInst.IsTombstoned != nil && vaultInst.IsTombstoned(chunkID) {
		o.replicationLogger.Debug("replication: chunk tombstoned after probe, aborting send",
			"vault", vaultID, "chunk", chunkID.String(), "node", nodeID)
		return nil
	}

	return o.chunkReplicator.ImportSealedChunk(ctx, nodeID, vaultID, chunkID, chunk.CursorIterator(cursor))
}
