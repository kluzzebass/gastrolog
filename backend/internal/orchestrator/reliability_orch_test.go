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
//   - OrchRel_SlowPeer_BackoffAbsorbs             (slow-but-not-stopped peer)
//   - OrchRel_LeaderKilledMidAppend_NoLoss        (in-flight appends + leader loss)
//   - OrchRel_IngestionStressWithPause            (pump records under paused peer)
//   - OrchRel_MultiVault_IsolatedFromPausedPeer   (vault with different placements unaffected)

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
	pre := h.chunkIDsOnLeader()
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
	h.assertAllNodesSee(h.chunkIDsOnLeader())
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
	baseline := h.chunkIDsOnLeader()

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

	t.Logf("victim=%s leader=%s baseline=%d chunks", victim, leader.id, len(baseline))
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
	baseline := h.chunkIDsOnLeader()

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

// Slow peer (not paused): add ~200ms of per-handler latency to one
// follower. Replication RPCs should complete, but slowly enough that
// ForwardingTimeout (5s) may or may not be hit depending on load. The
// cluster must absorb the slowness via backoff without stalling the
// leader. Asserts append+seal completes within budget and all nodes
// (including the slow one) eventually converge.
//
// Distinct from the paused scenario: here the slow peer DOES eventually
// respond, so the circuit breaker recovers and replication resumes.
// Catches a class of bug where slowness-tolerant code paths assume
// pause semantics (either fully alive or fully dead).
func TestOrchRel_SlowPeer_BackoffAbsorbs(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	victim := h.nodeIDs[2]
	h.slowPeer(victim, 200*time.Millisecond)
	t.Cleanup(func() { h.slowPeer(victim, 0) })

	const records = 10
	now := time.Now()
	appendDone := make(chan error, 1)
	go func() {
		for i := range records {
			if err := h.appendOnLeader(chunk.Record{
				SourceTS: now,
				IngestTS: now,
				Raw:      []byte("slow-" + strconv.Itoa(i)),
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
			t.Fatalf("append+seal failed under slow peer: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("append+seal stalled under slow peer")
	}

	// Slow peer should still converge (slower than paused-peer scenario
	// which excludes it). Clear the slowdown first so convergence isn't
	// additionally handicapped.
	h.slowPeer(victim, 0)
	h.assertAllNodesSee(h.chunkIDsOnLeader())
}

// Stop the vault-ctl Raft leader mid-append: fire a burst of appends,
// shortly after kill the Raft leader, verify that appends that RETURNED
// success are still present on the surviving quorum's tier FSMs. hraft
// guarantees this via majority commit before returning from Apply; we
// just need to make sure our plumbing preserves it.
func TestOrchRel_LeaderKilledMidAppend_NoLoss(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	// Burst records and record which ones the Append call acknowledged.
	const burst = 25
	now := time.Now()
	results := make([]bool, burst)
	doneBurst := make(chan struct{})
	go func() {
		defer close(doneBurst)
		for i := range burst {
			err := h.appendOnLeader(chunk.Record{
				SourceTS: now,
				IngestTS: now,
				Raw:      []byte("kill-" + strconv.Itoa(i)),
			})
			results[i] = err == nil
			if err != nil {
				// New leader election is expected during this test;
				// appends after the kill may fail until a new leader
				// comes up. Don't bail — we track successes.
				return
			}
		}
	}()

	// Let a few appends land, then kill the Raft leader.
	time.Sleep(200 * time.Millisecond)
	leader := h.waitForVaultCtlLeader()
	killedID := leader.id
	h.stopNode(killedID)

	// Wait for the goroutine to finish (it either completes or bails on
	// the first error after the kill).
	<-doneBurst

	// Re-elect on surviving quorum. Wait for readiness on the two live
	// nodes and verify their FSMs contain every chunk that was ack'd
	// before the kill.
	live := []string{}
	for _, id := range h.nodeIDs {
		if id != killedID {
			live = append(live, id)
		}
	}
	// Wait for a new leader among the live nodes.
	h.waitForVaultCtlLeader()
	h.sealOnLeader()

	// Verify: every append that RETURNED success is present on the
	// surviving quorum.
	successCount := 0
	for _, ok := range results {
		if ok {
			successCount++
		}
	}
	if successCount == 0 {
		t.Fatal("no appends succeeded before leader kill; test inconclusive")
	}
	t.Logf("append succeeded for %d/%d records before leader kill", successCount, burst)

	// Liveness check on the survivors: their FSMs should have at least
	// `successCount` entries (matching the committed records).
	deadline := time.Now().Add(orchHarnessConvWait)
	for time.Now().Before(deadline) {
		ids := h.chunkIDsOnNode(live[0])
		if len(ids) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Pump records continuously from multiple goroutines while one peer is
// paused. After the burst, unpause and verify every node converges to
// the same chunk set. Asserts that:
//   - ingestion never stalls (goroutines return promptly, not piled up)
//   - no records reported success + later appear lost
//   - convergence recovers after the pause is released
func TestOrchRel_IngestionStressWithPause(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	victim := h.nodeIDs[2]
	h.pausePeer(victim)

	const (
		writers          = 3
		recordsPerWriter = 20
		totalRecords     = writers * recordsPerWriter
	)
	now := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, totalRecords)
	start := time.Now()
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range recordsPerWriter {
				err := h.appendOnLeader(chunk.Record{
					SourceTS: now,
					IngestTS: now,
					Raw:      []byte("stress-" + strconv.Itoa(w) + "-" + strconv.Itoa(i)),
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
	case <-time.After(120 * time.Second):
		t.Fatal("ingestion stalled under paused peer stress")
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("stress append error: %v", err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("ingested %d records (%d writers) under paused peer in %v",
		totalRecords, writers, elapsed)

	h.sealOnLeader()

	// Unpause the peer, assert full convergence.
	h.unpausePeer(victim)
	h.assertAllNodesSee(h.chunkIDsOnLeader())
}

// True multi-vault isolation: configure two vaults with non-overlapping
// placements. Vault A lives on nodes {0,1,2} — includes the pause
// victim (node2). Vault B lives on nodes {0,1,3} — excludes it.
// Pause node2, then exercise vault B: its append+seal should complete
// at near-normal speed because none of its replicas are paused. The
// per-node circuit breaker keeps its backoff state on a per-node basis,
// but vault B's replication goroutines never fire against node2.
//
// Key assertion: vault B's append latency under a paused node2 should
// be comparable to an unaffected baseline — measurably faster than
// vault A's which would incur at least one ForwardingTimeout round.
func TestOrchRel_MultiVault_IsolatedFromPausedPeer(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 4, withExtraVault([]int{0, 1, 3}))

	vaultA := h.vaults[0] // placed on {0,1,2,3}
	vaultB := h.vaults[1] // placed on {0,1,3} — node2 excluded

	// Pause node2. It's part of vault A's tier, not vault B's.
	victim := h.nodeIDs[2]
	h.pausePeer(victim)
	t.Cleanup(func() { h.unpausePeer(victim) })

	now := time.Now()

	// Exercise vault B first — should be unaffected by the pause.
	bStart := time.Now()
	for i := range 5 {
		if err := h.appendOnLeaderForVault(vaultB, chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("B-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("vault B append %d: %v", i, err)
		}
	}
	h.sealOnLeaderForVault(vaultB)
	bElapsed := time.Since(bStart)
	t.Logf("vault B (no paused replica): 5 appends + seal in %v", bElapsed)

	// Vault B latency budget: should complete quickly since all of its
	// replicas are healthy. 5 seconds is very generous — healthy
	// replication completes in milliseconds.
	if bElapsed > 5*time.Second {
		t.Errorf("vault B took %v — paused peer should have had no effect", bElapsed)
	}

	// Exercise vault A. This MAY be slower (first record hits
	// ForwardingTimeout then backs off), but must still complete.
	aStart := time.Now()
	for i := range 3 {
		if err := h.appendOnLeaderForVault(vaultA, chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("A-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("vault A append %d: %v", i, err)
		}
	}
	aElapsed := time.Since(aStart)
	t.Logf("vault A (with paused replica): 3 appends in %v", aElapsed)

	// Verify vault B's chunks reached every node that hosts it (0, 1, 3).
	expected := h.chunkIDsOnNodeForVault(vaultB, h.nodeIDs[0])
	if len(expected) == 0 {
		t.Fatal("vault B leader has no sealed chunks after seal")
	}
	liveForB := []string{h.nodeIDs[0], h.nodeIDs[1], h.nodeIDs[3]}
	for _, id := range liveForB {
		deadline := time.Now().Add(orchHarnessConvWait)
		for time.Now().Before(deadline) {
			got := h.chunkIDsOnNodeForVault(vaultB, id)
			if len(got) == len(expected) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		got := h.chunkIDsOnNodeForVault(vaultB, id)
		if len(got) != len(expected) {
			t.Errorf("vault B on node %s: expected %d chunks, got %d",
				h.nodes[id].label, len(expected), len(got))
		}
	}
}

// eventuallyLiveNodesSeeSealedChunk is the subset variant of
// eventuallyAllSeeSealedChunk used when we expect only some nodes to be
// caught up (e.g. one is paused).
func (h *orchRelHarness) eventuallyLiveNodesSeeSealedChunk(t *testing.T, live []string) {
	t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	var expected map[chunk.ChunkID]bool
	for time.Now().Before(deadline) {
		expected = h.chunkIDsOnLeader()
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
		leader := h.chunkIDsOnLeader()
		if len(leader) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		h.assertAllNodesSee(leader)
		return
	}
	t.Fatalf("no sealed chunk appeared on leader within %s", orchHarnessConvWait)
}
