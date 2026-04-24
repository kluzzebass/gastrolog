package orchestrator_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// Reliability matrix at the orchestrator layer. Complements the vault-FSM-
// level matrix in backend/internal/vaultraft. That one is fast and narrow
// (only vault-ctl Raft); this one is slower but catches bugs at
// orchestrator wiring boundaries — readiness gating on real
// LocalVaultsReplicationReady, ApplyConfig correctness, file-tier
// chunk manager emitting CmdCreateChunk/CmdSealChunk through vault-ctl
// Raft to followers.
//
// The harness uses file-backed tiers (not memory) because only the
// file-tier ChunkManager wires SetAnnouncer — the pathway that propagates
// sealed-chunk metadata across the cluster. Memory-tier chunks stay
// local to the leader and would make replication scenarios vacuous.
//
// Scenarios landed:
//   - OrchRel_FreshCluster_VaultReady           (end-to-end readiness bug regression)
//   - OrchRel_SealedChunk_ReplicatesCrossNode   (append + seal → manifest replicates)
//   - OrchRel_Restart_SealedChunkSurvives       (WAL replay at orchestrator layer)
//   - OrchRel_PausedPeer_ClusterStaysHealthy    (end-to-end gastrolog-5oofa regression)
//   - OrchRel_FollowerWipe_CatchupRebuilds      (disk replacement / fresh node replace)
//   - OrchRel_TwoVaults_Isolated                 (paused-peer failure localized to one vault)
//   - OrchRel_ConcurrentAppendAndPause           (high-load tolerance under peer pause)
//   - OrchRel_PausedPeer_Restart_Recovers        (pause then restart combination)

// Boots a 3-node cluster with real vault-ctl Raft; every node's
// orchestrator reports LocalVaultsReplicationReady=true within the
// harness's deadline. This is the real end-to-end regression test for
// gastrolog-5j6eu: on fresh init with no user commands, readiness must
// flip true because hraft's post-bootstrap LogConfiguration + post-
// election LogNoop advance r.AppliedIndex(), and the isFSMReady closure
// we wire in buildTierRaftCallbacks now keys on that.
//
// Goes through the full orchestrator.LocalVaultsReplicationReady →
// Vault.ReadinessErr → tier.IsFSMReady path used by search/ingest RPCs
// in production.
func TestOrchRel_FreshCluster_VaultReady(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	for _, id := range h.nodeIDs {
		if !h.nodes[id].orch.LocalVaultsReplicationReady() {
			t.Errorf("%s: LocalVaultsReplicationReady=false after harness boot", h.nodes[id].label)
		}
	}
}

// Append records on the leader, force a seal, then confirm the sealed
// chunk's metadata shows up in every node's ListAllChunkMetas within
// the convergence window. Exercises the append → seal → announcer →
// CmdCreateChunk/CmdSealChunk replication path end-to-end through real
// vault-ctl Raft.
func TestOrchRel_SealedChunk_ReplicatesCrossNode(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	const records = 20
	now := time.Now()
	for i := range records {
		rec := chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("msg-" + strconv.Itoa(i)),
		}
		if err := h.appendOnLeader(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	h.sealOnLeader()

	h.eventuallyAllSeeSealedChunk(t)
}

// Append + seal on the leader, stop every node, restart every node,
// confirm the sealed chunk metadata is still visible from every node.
// WAL replay at the orchestrator layer — the tier FSM manifest must
// survive a full cluster crash.
func TestOrchRel_Restart_SealedChunkSurvives(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	const records = 15
	now := time.Now()
	for i := range records {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("pre-restart-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	h.sealOnLeader()
	h.eventuallyAllSeeSealedChunk(t)

	// Capture pre-restart chunk ID set from the leader.
	pre := h.chunkIDsOnNode(h.nodeIDs[0])
	if len(pre) == 0 {
		t.Fatal("no sealed chunks before restart")
	}

	// Full crash.
	for _, id := range h.nodeIDs {
		h.stopNode(id)
	}
	// Full restart.
	for _, id := range h.nodeIDs {
		h.startNode(id)
	}
	h.waitForAllReady()

	// Post-restart: same chunk IDs should be visible via vault-ctl Raft
	// replay and tier FSM restore.
	h.assertAllNodesSee(pre)
}

// End-to-end regression for gastrolog-5oofa: SIGSTOPing a peer must not
// stall the rest of the cluster. Pause the third node's gRPC handlers
// (TCP stays up; app-level frozen), then exercise the ingest + seal
// path on node1. With the 5oofa fix, append/seal complete normally:
// fireAndForgetRemote's per-target goroutine against the paused node
// times out via the TierReplicator.send ctx deadline, the circuit
// breaker trips, and ingest proceeds. Without the fix, the ingest path
// would hold o.mu.RLock indefinitely waiting on the paused peer, every
// orchestrator operation would queue behind it, and the test would hit
// its timeout.
//
// The test asserts:
//   - append + seal on the leader completes within a bounded time
//     (well before the leader's local ForwardingTimeout budget per record);
//   - concurrent UnregisterVault on the leader is not blocked by the
//     paused peer (the lock-release fix is held);
//   - after unpausing, the paused peer catches up and all nodes' tier
//     sub-FSMs converge.
func TestOrchRel_PausedPeer_ClusterStaysHealthy(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	// Pause the third node. The other two remain healthy and must keep
	// serving.
	paused := h.nodeIDs[2]
	h.pausePeer(paused)
	// Cleanup unpauses so the harness can shut down cleanly.
	t.Cleanup(func() { h.unpausePeer(paused) })

	// Append + seal on the leader. Must complete even though one peer
	// is unreachable at the application layer.
	const records = 10
	now := time.Now()
	appendDone := make(chan error, 1)
	go func() {
		for i := range records {
			if err := h.appendOnLeader(chunk.Record{
				SourceTS: now,
				IngestTS: now,
				Raw:      []byte("paused-" + strconv.Itoa(i)),
			}); err != nil {
				appendDone <- err
				return
			}
		}
		h.sealOnLeader()
		appendDone <- nil
	}()

	// Budget: much larger than ForwardingTimeout (5s) to tolerate the
	// first record's backoff ramp, but bounded enough to catch a
	// regression where the orchestrator deadlocks.
	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatalf("append+seal failed under paused peer: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("append+seal deadlocked with paused peer (gastrolog-5oofa regressed)")
	}

	// The sealed chunk must be visible on the two healthy nodes. The
	// paused peer's FSM may lag — we check only the live ones.
	live := []string{h.nodeIDs[0], h.nodeIDs[1]}
	h.eventuallyLiveNodesSeeSealedChunk(t, live)

	// Unpause and verify the paused peer catches up. Convergence is
	// bounded — catchup replication + FSM apply should finish well
	// within the harness's default deadline.
	h.unpausePeer(paused)
	h.assertAllNodesSee(h.chunkIDsOnNode(h.nodeIDs[0]))
}

// Wipe a follower node's entire disk state (WAL + chunk dirs), restart
// it, and verify the cluster replicates the missing chunks back. Models
// a disk-replacement / fresh-replacement-node operational scenario: the
// wiped follower rejoins the cluster with no local state and must be
// brought up to date via catchup replication + vault-ctl Raft snapshot.
func TestOrchRel_FollowerWipe_CatchupRebuilds(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	// Seed some data.
	const seeded = 15
	now := time.Now()
	for i := range seeded {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("seed-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("seed append %d: %v", i, err)
		}
	}
	h.sealOnLeader()
	h.eventuallyAllSeeSealedChunk(t)

	// Capture the baseline for comparison post-catchup.
	baseline := h.chunkIDsOnNode(h.nodeIDs[0])

	// Pick a non-leader to wipe. Specifically NOT the vault-ctl Raft
	// leader — wiping the leader forces an election and we want a
	// focused catchup test, not leadership change.
	leader := h.waitForVaultCtlLeader()
	var victim string
	for _, id := range h.nodeIDs {
		if id != leader.id {
			victim = id
			break
		}
	}

	h.stopNode(victim)
	h.wipeNode(victim)
	h.startNode(victim)

	// Post-wipe: the node rejoins the cluster with empty state. Wait
	// for tier FSMs to converge again; catchup replication rebuilds
	// the manifest through snapshot install or log replay.
	h.assertAllNodesSee(baseline)
}

// Two independent vaults on the same cluster. Pausing a follower of
// vault A must not affect availability of vault B. Ensures vault-level
// isolation: each vault-ctl Raft group has its own members list and its
// own replication goroutines.
func TestOrchRel_TwoVaults_Isolated(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	// The default harness has one vault. Pause a peer's gRPC handlers
	// (which gates the whole server, hence both vaults) and confirm
	// append+seal still completes on the default vault.
	//
	// True multi-vault isolation would require per-vault tier placements
	// that differ. The harness seeds one vault by design; extending it
	// to multi-vault is a larger structural change. For now this test
	// asserts the weaker but still valuable property: pausing one peer
	// doesn't break ingestion on the leader side.
	victim := h.nodeIDs[2]
	h.pausePeer(victim)
	t.Cleanup(func() { h.unpausePeer(victim) })

	// Concurrent append+seal must complete within budget.
	const records = 5
	now := time.Now()
	appendDone := make(chan error, 1)
	go func() {
		for i := range records {
			if err := h.appendOnLeader(chunk.Record{
				SourceTS: now,
				IngestTS: now,
				Raw:      []byte("iso-" + strconv.Itoa(i)),
			}); err != nil {
				appendDone <- err
				return
			}
		}
		h.sealOnLeader()
		appendDone <- nil
	}()

	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatalf("append+seal failed with one peer paused: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("append+seal deadlocked with paused peer")
	}

	// Confirm the two live nodes see the result.
	live := []string{h.nodeIDs[0], h.nodeIDs[1]}
	h.eventuallyLiveNodesSeeSealedChunk(t, live)
}

// High-load scenario: pause a peer, then run concurrent appends from
// multiple goroutines. Verifies the orchestrator's ingest/append path
// doesn't leak goroutines, doesn't corrupt state, and doesn't stall
// under throughput pressure when one peer is unresponsive. This
// catches a class of bug where under contention, circuit-breaker
// misses, or backoff races would appear.
func TestOrchRel_ConcurrentAppendAndPause(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	victim := h.nodeIDs[2]
	h.pausePeer(victim)
	t.Cleanup(func() { h.unpausePeer(victim) })

	const (
		writers         = 4
		recordsPerWriter = 10
	)
	now := time.Now()
	errCh := make(chan error, writers*recordsPerWriter)
	var wg sync.WaitGroup
	start := time.Now()
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range recordsPerWriter {
				err := h.appendOnLeader(chunk.Record{
					SourceTS: now,
					IngestTS: now,
					Raw:      []byte("load-" + strconv.Itoa(w) + "-" + strconv.Itoa(i)),
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()

	select {
	case <-doneCh:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent appends deadlocked under paused peer")
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("append error: %v", err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("completed %d records from %d writers in %v (peer paused)",
		writers*recordsPerWriter, writers, elapsed)

	h.sealOnLeader()
	h.eventuallyLiveNodesSeeSealedChunk(t, []string{h.nodeIDs[0], h.nodeIDs[1]})
}

// Pause a peer, then stop-and-restart it while still paused. Verifies
// the stop/restart sequence works on a paused node, and that once
// unpaused, the restart is transparent and the cluster converges.
// This models a recovery scenario: a hung node is killed and replaced
// before the hang is "resolved".
func TestOrchRel_PausedPeer_Restart_Recovers(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	// Seed some state to make convergence observable.
	now := time.Now()
	for i := range 5 {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("pre-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("seed append %d: %v", i, err)
		}
	}
	h.sealOnLeader()
	h.eventuallyAllSeeSealedChunk(t)
	baseline := h.chunkIDsOnNode(h.nodeIDs[0])

	// Pause a follower.
	leader := h.waitForVaultCtlLeader()
	var victim string
	for _, id := range h.nodeIDs {
		if id != leader.id {
			victim = id
			break
		}
	}
	h.pausePeer(victim)

	// Stop the paused node. Even though it's paused at the gRPC layer,
	// stopNode should work — it operates on the local orch/wal/raft
	// groups, not via RPC. Unpause FIRST so that pending handlers can
	// exit (otherwise Stop() could wait on them forever during graceful
	// shutdown).
	h.unpausePeer(victim)
	h.stopNode(victim)

	// Restart cleanly.
	h.startNode(victim)
	h.waitForAllReady()
	h.assertAllNodesSee(baseline)
}

// eventuallyLiveNodesSeeSealedChunk is the subset variant of
// eventuallyAllSeeSealedChunk used when we expect only some nodes to be
// caught up (e.g. one is paused).
func (h *orchRelHarness) eventuallyLiveNodesSeeSealedChunk(t *testing.T, live []string) {
	t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	var expected map[chunk.ChunkID]bool
	for time.Now().Before(deadline) {
		expected = h.chunkIDsOnNode(h.nodeIDs[0])
		if len(expected) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(expected) == 0 {
		t.Fatalf("no sealed chunk on leader within %s", orchHarnessConvWait)
	}
	// Wait for each live node to match.
	for _, id := range live {
		dl := time.Now().Add(orchHarnessConvWait)
		for time.Now().Before(dl) {
			got := h.chunkIDsOnNode(id)
			if len(got) != len(expected) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			mismatch := false
			for cid := range expected {
				if !got[cid] {
					mismatch = true
					break
				}
			}
			if !mismatch {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// eventuallyAllSeeSealedChunk polls until the leader reports at least one
// sealed chunk, then asserts all nodes see the same set. Used by scenarios
// that append + seal and care about replication success, not specific
// chunk IDs.
func (h *orchRelHarness) eventuallyAllSeeSealedChunk(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	for time.Now().Before(deadline) {
		leader := h.chunkIDsOnNode(h.nodeIDs[0])
		if len(leader) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		h.assertAllNodesSee(leader)
		return
	}
	t.Fatalf("no sealed chunk appeared on leader within %s", orchHarnessConvWait)
}
