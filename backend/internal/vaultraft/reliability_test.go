package vaultraft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/tierfsm"

	hraft "github.com/hashicorp/raft"
)

// Reliability matrix for the vault control-plane Raft group. Each test
// exercises a failure axis against the full vaultraft.FSM + raftwal + hraft
// stack. Use the harness defined in reliability_harness_test.go.
//
// Scenario coverage:
//   - FreshCluster_AppliedIndexNonZero     (regression for gastrolog-5j6eu readiness bug)
//   - FreshCluster_FSMsConvergeEmpty
//   - LeaderApply_ReplicatesToFollowers
//   - Restart_AllNodes_ChunkStateSurvives
//   - Failover_LeaderDown_NewLeaderElected
//   - Failover_FollowerDown_QuorumHolds
//   - Partition_MinorityBlocked_HealReconverges
//   - WholeClusterCrash_WALReplayRestoresState (durability: log survives all-nodes-down)
//   - ConcurrentWrites_NoDivergence             (many-writer load)
//   - FollowerWipe_CatchupViaSnapshotOrReplay  (log-replay path)
//   - SnapshotInstall_CatchesUpWipedFollower   (InstallSnapshot path)
//   - PipelinedApplies_SurviveLeaderKill       (pipelined futures + leader loss)
//   - RapidLeaderRestart_NoDivergence           (stress: repeated restarts)
//   - MultipleVaults_IsolatedAndConvergent      (two vault FSMs side by side)
//   - SnapshotCycleUnderLoad_NoCorruption       (Snapshot() races with Apply)
//   - LargeFSM_SnapshotRestoreRoundtrip          (stress streaming Restore)
//
// Future direction: a full orchestrator-backed harness that boots N real
// orchestrators with Raft-backed tiers (GroupManager + raftwal +
// multiraft.Transport on loopback gRPC). That unlocks end-to-end
// scenarios — ingest, seal, search across nodes during failover —
// instead of testing the vault control-plane Raft in isolation. Tracked
// as a follow-up to gastrolog-5ff7z because the wiring (gRPC servers per
// node, chunk/index factories, scheduler lifecycle, cross-node
// forwarders) is substantial and deserves its own branch.
//
// The current harness runs only the vault control-plane Raft — enough
// to catch metadata-divergence bugs but not orchestrator-layer
// regressions (like the GetFields UTF-8 bug filed as gastrolog-1uh5h,
// which lives above this layer).

// On a freshly bootstrapped vault-ctl Raft group — no user commands,
// no FSM Apply calls — hraft still commits the bootstrap LogConfiguration
// and the leader's post-election LogNoop. r.AppliedIndex() must advance
// past zero on every node, including followers. This is the invariant the
// readiness gate (orchestrator/vault_readiness.go) keys on.
//
// Before gastrolog-5j6eu's fix, readiness keyed on FSM.Ready() which only
// flips on LogCommand entries, wedging every fresh vault as "not ready".
func TestReliability_FreshCluster_AppliedIndexNonZero(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	// Wait for full replication of bootstrap + post-election entries.
	deadline := time.Now().Add(2 * time.Second)
	var lastApplied map[string]uint64
	for time.Now().Before(deadline) {
		lastApplied = map[string]uint64{}
		allReady := true
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			if n.raft == nil {
				continue
			}
			ai := n.raft.AppliedIndex()
			lastApplied[id] = ai
			if ai == 0 {
				allReady = false
			}
		}
		if allReady {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not all nodes advanced past AppliedIndex=0: %v", lastApplied)
}

// A fresh cluster with no commands must have empty but convergent FSMs
// across all nodes. This is the positive-space companion to the applied
// index check: no divergence even when no user state exists.
func TestReliability_FreshCluster_FSMsConvergeEmpty(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)
	h.assertAllFSMsConverged()
}

// Leader applies a tier command; every follower must converge to the same
// FSM state. Baseline end-to-end replication check.
func TestReliability_LeaderApply_ReplicatesToFollowers(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0xA1), now)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0xB2), now)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0xC3), now)

	h.assertAllFSMsConverged()
}

// All three nodes restart in sequence. After each restart, the node
// rejoins, replays its WAL, and converges with the rest. End-state FSM
// must equal pre-restart state.
func TestReliability_Restart_AllNodes_ChunkStateSurvives(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x01), now)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x02), now)
	h.assertAllFSMsConverged()

	// Capture the pre-restart fingerprint (any node, since they're converged).
	expected := vaultFSMFingerprint(h.nodes[h.nodeIDs[0]].fsm)

	for _, id := range h.nodeIDs {
		h.restartNode(id)
		// Wait for leader — election may re-run if the restarted node was leader.
		h.waitForLeader()
	}
	h.assertAllFSMsConverged()

	got := vaultFSMFingerprint(h.nodes[h.leaderID()].fsm)
	if got != expected {
		t.Fatalf("post-restart fingerprint differs:\nexpected:\n%s\ngot:\n%s", expected, got)
	}
}

// Stop the leader. Remaining two nodes must elect a new leader and keep
// making progress. The original leader's state is expected to match the
// new leader's (it had the same log).
func TestReliability_Failover_LeaderDown_NewLeaderElected(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x10), now)
	h.assertAllFSMsConverged()

	oldLeader := h.leaderID()
	h.stopNode(oldLeader)

	// Wait for a new leader among remaining nodes.
	deadline := time.Now().Add(harnessLeaderWait)
	var newLeader string
	for time.Now().Before(deadline) {
		id := h.leaderID()
		if id != "" && id != oldLeader {
			newLeader = id
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatalf("no new leader elected after %s went down", oldLeader)
	}

	// New leader accepts writes.
	leader := h.nodes[newLeader]
	cmd := MarshalVaultChunkCommand(tierID, tierfsm.MarshalCreateChunk(chunkIDWithPrefix(0x11), now, now, now))
	if err := leader.raft.Apply(cmd, 2*time.Second).Error(); err != nil {
		t.Fatalf("apply under new leader: %v", err)
	}

	// Two-node quorum converges.
	liveIDs := []string{}
	for _, id := range h.nodeIDs {
		if id != oldLeader {
			liveIDs = append(liveIDs, id)
		}
	}
	assertSubsetConverged(t, h, liveIDs)
}

// Follower shutdown must not stall writes: 2/3 is still quorum.
func TestReliability_Failover_FollowerDown_QuorumHolds(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	// Pick a follower (any non-leader node).
	leaderID := h.leaderID()
	var follower string
	for _, id := range h.nodeIDs {
		if id != leaderID {
			follower = id
			break
		}
	}
	h.stopNode(follower)

	// Writes still commit under 2-node quorum.
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x20), now)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x21), now)

	liveIDs := []string{}
	for _, id := range h.nodeIDs {
		if id != follower {
			liveIDs = append(liveIDs, id)
		}
	}
	assertSubsetConverged(t, h, liveIDs)
}

// Partition one node from the other two. The isolated minority must lose
// leadership if it held it and stop committing. After healing, the
// minority catches up and FSMs reconverge.
func TestReliability_Partition_MinorityBlocked_HealReconverges(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x30), now)
	h.assertAllFSMsConverged()

	leaderID := h.leaderID()
	var isolated string
	for _, id := range h.nodeIDs {
		if id != leaderID {
			isolated = id
			break
		}
	}

	// Partition: disconnect isolated from both others.
	for _, id := range h.nodeIDs {
		if id != isolated {
			h.disconnect(isolated, id)
		}
	}

	// Majority continues to accept writes.
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x31), now)
	h.applyTierCreate(tierID, chunkIDWithPrefix(0x32), now)

	// Heal the partition.
	for _, id := range h.nodeIDs {
		if id != isolated {
			h.reconnect(isolated, id)
		}
	}

	// Isolated node catches up; all three converge.
	h.assertAllFSMsConverged()
}

// Stop every node simultaneously (simulating a power loss / cluster-wide
// crash), then restart everyone. All previously-committed entries must
// reappear via WAL replay. Stronger than the rolling-restart test because
// there's no surviving replica to catch up from — every node must restore
// purely from its own WAL.
func TestReliability_WholeClusterCrash_WALReplayRestoresState(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	for i := range byte(5) {
		h.applyTierCreate(tierID, chunkIDWithPrefix(0x40+i), now)
	}
	h.assertAllFSMsConverged()

	expected := vaultFSMFingerprint(h.nodes[h.nodeIDs[0]].fsm)

	// All nodes down at once. stopNode closes the WAL with fsync, so the log
	// on disk reflects everything that was committed.
	for _, id := range h.nodeIDs {
		h.stopNode(id)
	}
	// All nodes back up. Each reopens its WAL and replays locally; hraft
	// then re-establishes consensus from the log state each node has.
	for _, id := range h.nodeIDs {
		h.startNode(id)
	}
	h.wireTransports()
	h.waitForLeader()
	h.assertAllFSMsConverged()

	got := vaultFSMFingerprint(h.nodes[h.leaderID()].fsm)
	if got != expected {
		t.Fatalf("post-crash fingerprint differs:\nexpected:\n%s\ngot:\n%s", expected, got)
	}
}

// Many concurrent writers pushing to different tiers. All FSMs must
// converge to a state that contains exactly the total number of commands
// issued, and every chunk ID shows up on every node.
//
// Any state divergence after convergence (duplicate apply, missing apply,
// reordered apply that changes observable flags) fails the fingerprint
// check.
func TestReliability_ConcurrentWrites_NoDivergence(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	const (
		writers          = 4
		commandsPerWriter = 25
	)

	tierIDs := make([]glid.GLID, writers)
	for i := range writers {
		tierIDs[i] = glid.New()
	}

	now := time.Now().Truncate(time.Nanosecond)
	var wg sync.WaitGroup
	errCh := make(chan error, writers*commandsPerWriter)
	for w := range writers {
		wg.Add(1)
		go func(writerIdx int) {
			defer wg.Done()
			tierID := tierIDs[writerIdx]
			for c := range commandsPerWriter {
				// Unique chunk ID: writer index in byte 0, command index in byte 1.
				var cid chunk.ChunkID
				cid[0] = byte(writerIdx)
				cid[1] = byte(c)
				wire := tierfsm.MarshalCreateChunk(cid, now, now, now)
				cmd := MarshalVaultChunkCommand(tierID, wire)
				if err := applyWithLeaderRetry(h, cmd, 5, 3*time.Second); err != nil {
					errCh <- fmt.Errorf("writer %d cmd %d: %w", writerIdx, c, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	h.assertAllFSMsConverged()

	// Cross-check: leader's FSM has exactly writers*commandsPerWriter entries.
	leader := h.nodes[h.leaderID()]
	total := 0
	for _, tid := range tierIDs {
		if sub := leader.fsm.TierFSM(tid); sub != nil {
			total += len(sub.List())
		}
	}
	if total != writers*commandsPerWriter {
		t.Fatalf("expected %d entries in leader FSM, got %d", writers*commandsPerWriter, total)
	}
}

// Wipe a follower's WAL while the leader continues to commit, then restart
// the follower. The follower must rejoin the cluster with empty local state
// and catch up either via snapshot install (if the leader has taken a
// snapshot) or log replay from the leader's retained tail. End-state FSM
// must match the leader.
//
// This is the "new voter catches up from scratch" path, exercised on an
// existing voter via wipe-and-restart. Real cluster operations (node
// replacement, disk failure recovery) share this path.
func TestReliability_FollowerWipe_CatchupViaSnapshotOrReplay(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	// Seed some pre-wipe state.
	for i := range byte(3) {
		h.applyTierCreate(tierID, chunkIDWithPrefix(0x50+i), now)
	}
	h.assertAllFSMsConverged()

	// Pick a follower.
	leaderID := h.leaderID()
	var followerID string
	for _, id := range h.nodeIDs {
		if id != leaderID {
			followerID = id
			break
		}
	}

	// Wipe: stop, delete the WAL dir, start again pointing at the same path
	// (which startNode will recreate empty).
	h.stopNode(followerID)
	if err := removeDirContents(h.nodes[followerID].walDir); err != nil {
		t.Fatalf("wipe wal dir: %v", err)
	}
	h.startNode(followerID)
	h.wireTransports()

	// Keep applying on the leader while the wiped follower catches up.
	// This also forces the log past what the wiped follower had before,
	// proving catch-up includes post-wipe entries.
	for i := range byte(3) {
		h.applyTierCreate(tierID, chunkIDWithPrefix(0x60+i), now)
	}

	h.assertAllFSMsConverged()
}

// Like FollowerWipe, but first forces a snapshot on the leader and
// truncates the log behind it. The wiped follower then cannot catch up
// via log replay alone — it must receive an InstallSnapshot RPC and
// Restore the vault FSM from the snapshot bytes.
//
// This exercises vaultraft.FSM.Snapshot + Restore end-to-end through
// hraft's snapshot-install path. The streaming Restore fix landed in
// gastrolog-5j6eu lives here too.
func TestReliability_SnapshotInstall_CatchesUpWipedFollower(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	// Phase 1: seed many entries so a snapshot has meaningful content.
	for i := range byte(20) {
		h.applyTierCreate(tierID, chunkIDWithPrefix(0x70+i), now)
	}
	h.assertAllFSMsConverged()

	// Force the leader to take a snapshot. hraft's Snapshot() returns when
	// the snapshot is persisted AND the log has been truncated past it
	// (retaining only TrailingLogs entries).
	leaderID := h.leaderID()
	if err := h.nodes[leaderID].raft.Snapshot().Error(); err != nil {
		t.Fatalf("force snapshot: %v", err)
	}

	// Phase 2: apply more entries after the snapshot so the snapshot is
	// behind the leader's LastIndex. Catch-up must still work.
	for i := range byte(5) {
		h.applyTierCreate(tierID, chunkIDWithPrefix(0x90+i), now)
	}

	// Wipe a follower.
	var followerID string
	for _, id := range h.nodeIDs {
		if id != leaderID {
			followerID = id
			break
		}
	}
	h.stopNode(followerID)
	if err := removeDirContents(h.nodes[followerID].walDir); err != nil {
		t.Fatalf("wipe wal dir: %v", err)
	}
	h.startNode(followerID)
	h.wireTransports()

	// The wiped follower's log starts at 0; the leader's earliest log
	// entry is post-snapshot. hraft must install the snapshot, then
	// replay the post-snapshot tail.
	h.assertAllFSMsConverged()
}

// Pipelined Apply futures — fire many without waiting, then kill the
// leader mid-burst. Futures that returned success before the kill must
// be durably applied on the surviving quorum; futures that errored are
// free to have any state. Surviving nodes' FSMs must converge.
//
// This catches torn-commit bugs: cases where a leader acknowledges an
// Apply locally but the replication to majority hadn't happened before
// the kill. hraft is supposed to prevent this (Apply returns only after
// majority commit) — this scenario locks in that contract.
func TestReliability_PipelinedApplies_SurviveLeaderKill(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	// Build up a confirmed baseline.
	h.applyTierCreate(tierID, chunkIDWithPrefix(0xA0), now)
	h.assertAllFSMsConverged()

	// Pipeline 20 Apply futures on the current leader. Collect success
	// or error for each; do not block waiting on majority commit.
	oldLeader := h.leaderID()
	type result struct {
		cid chunk.ChunkID
		err error
	}
	resultsCh := make(chan result, 20)

	var wg sync.WaitGroup
	for i := range byte(20) {
		cid := chunkIDWithPrefix(0xB0 + i)
		wg.Add(1)
		go func(cid chunk.ChunkID) {
			defer wg.Done()
			cmd := MarshalVaultChunkCommand(tierID, tierfsm.MarshalCreateChunk(cid, now, now, now))
			n := h.nodes[oldLeader]
			n.mu.Lock()
			r := n.raft
			n.mu.Unlock()
			if r == nil {
				resultsCh <- result{cid, fmt.Errorf("leader gone")}
				return
			}
			err := r.Apply(cmd, 2*time.Second).Error()
			resultsCh <- result{cid, err}
		}(cid)
	}

	// Let a few commit, then yank the leader.
	time.Sleep(50 * time.Millisecond)
	h.stopNode(oldLeader)

	wg.Wait()
	close(resultsCh)

	confirmed := make(map[chunk.ChunkID]bool)
	for r := range resultsCh {
		if r.err == nil {
			confirmed[r.cid] = true
		}
	}

	// Wait for new leadership on the surviving pair.
	deadline := time.Now().Add(harnessLeaderWait)
	var newLeader string
	for time.Now().Before(deadline) {
		id := h.leaderID()
		if id != "" && id != oldLeader {
			newLeader = id
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatalf("no new leader after killing %s", oldLeader)
	}

	liveIDs := []string{}
	for _, id := range h.nodeIDs {
		if id != oldLeader {
			liveIDs = append(liveIDs, id)
		}
	}
	assertSubsetConverged(t, h, liveIDs)

	// Every chunk that Apply confirmed must be in the surviving leader's FSM.
	surviving := h.nodes[newLeader].fsm.TierFSM(tierID)
	if surviving == nil {
		t.Fatal("surviving leader lost tier FSM entirely")
	}
	for cid := range confirmed {
		if surviving.Get(cid) == nil {
			t.Errorf("confirmed chunk %x missing after leader kill", cid[:4])
		}
	}
}

// Restart the current leader repeatedly (5 times). Each restart forces
// a re-election; the cluster must converge between each restart and the
// final FSM state must contain all pre-restart entries.
//
// Stresses the interplay between leader election, WAL replay on a
// restarting leader, and log catch-up from peers.
func TestReliability_RapidLeaderRestart_NoDivergence(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	for i := range byte(3) {
		h.applyTierCreate(tierID, chunkIDWithPrefix(0xC0+i), now)
	}
	h.assertAllFSMsConverged()

	for range 5 {
		leader := h.leaderID()
		h.restartNode(leader)
		h.waitForLeader()
		h.assertAllFSMsConverged()
	}

	// Drop a final entry and confirm it replicates.
	h.applyTierCreate(tierID, chunkIDWithPrefix(0xCF), now)
	h.assertAllFSMsConverged()

	// Leader's FSM should have 3 original + 1 final = 4 entries.
	leader := h.nodes[h.leaderID()]
	sub := leader.fsm.TierFSM(tierID)
	if sub == nil {
		t.Fatal("tier FSM missing after restarts")
	}
	if got := len(sub.List()); got != 4 {
		t.Errorf("expected 4 chunks, got %d", got)
	}
}

// Two independent vault FSMs cohabiting the same cluster must converge
// independently without cross-contamination. Validates the tier-ID
// keying inside vaultraft.FSM: commands for tier A must not affect
// tier B's sub-FSM.
//
// This is the "one vault-ctl group per vault" model in miniature — the
// production deployment runs N vault-ctl groups per node, each with its
// own tier sub-FSMs.
func TestReliability_MultipleVaults_IsolatedAndConvergent(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierA := glid.New()
	tierB := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	h.applyTierCreate(tierA, chunkIDWithPrefix(0xD0), now)
	h.applyTierCreate(tierA, chunkIDWithPrefix(0xD1), now)
	h.applyTierCreate(tierB, chunkIDWithPrefix(0xE0), now)
	h.applyTierCreate(tierB, chunkIDWithPrefix(0xE1), now)
	h.applyTierCreate(tierB, chunkIDWithPrefix(0xE2), now)

	h.assertAllFSMsConverged()

	leader := h.nodes[h.leaderID()]
	subA := leader.fsm.TierFSM(tierA)
	subB := leader.fsm.TierFSM(tierB)
	if subA == nil || subB == nil {
		t.Fatal("missing tier sub-FSM")
	}
	if got := len(subA.List()); got != 2 {
		t.Errorf("tier A: expected 2 chunks, got %d", got)
	}
	if got := len(subB.List()); got != 3 {
		t.Errorf("tier B: expected 3 chunks, got %d", got)
	}

	// Cross-check isolation: tier A must not contain tier B's chunks.
	for _, e := range subA.List() {
		if e.ID[0] >= 0xE0 {
			t.Errorf("tier A leaked tier B chunk: %x", e.ID[:4])
		}
	}
}

// Force repeated snapshots on the leader while Apply traffic is in flight.
// Snapshot() truncates the log past the snapshot point; if that interacts
// badly with a concurrent StoreLog, the follower's log could end up with a
// gap or a torn tail and divergence would surface as a fingerprint mismatch.
//
// hraft coordinates Snapshot/Apply internally but the raftwal + hraft
// combination is project-specific, so this scenario is where "somebody
// else's bug becomes our bug" gets caught.
func TestReliability_SnapshotCycleUnderLoad_NoCorruption(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 3)

	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	// Apply workload: 40 commands, with Snapshot() forced every 10 commands.
	const (
		totalCommands = 40
		snapEvery     = 10
	)

	for i := range byte(totalCommands) {
		h.applyTierCreate(tierID, chunkIDWithPrefix(i), now)
		if (i+1)%snapEvery == 0 {
			if err := h.nodes[h.leaderID()].raft.Snapshot().Error(); err != nil {
				// ErrNothingNewToSnapshot is benign (snapshot cycle may overlap).
				if !isBenignSnapshotErr(err) {
					t.Fatalf("force snapshot after cmd %d: %v", i, err)
				}
			}
		}
	}

	h.assertAllFSMsConverged()

	leader := h.nodes[h.leaderID()].fsm.TierFSM(tierID)
	if leader == nil {
		t.Fatal("tier missing after snapshot cycle")
	}
	if got := len(leader.List()); got != totalCommands {
		t.Errorf("expected %d entries, got %d", totalCommands, got)
	}
}

func isBenignSnapshotErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, hraft.ErrNothingNewToSnapshot)
}

// Build a large FSM (many tiers × many chunks), take a snapshot, Restore
// into a fresh FSM from the snapshot bytes, compare fingerprints. This
// is the streaming Restore path from gastrolog-5j6eu under a realistic
// payload size — the io.ReadFull / io.LimitReader framing must stay
// aligned across many tier blobs.
//
// Runs entirely in-process (no Raft) to isolate the Snapshot+Restore
// contract from replication concerns.
func TestReliability_LargeFSM_SnapshotRestoreRoundtrip(t *testing.T) {
	t.Parallel()

	src := NewFSM()
	now := time.Now().Truncate(time.Nanosecond)

	const (
		numTiers         = 20
		chunksPerTier    = 30
	)
	tiers := make([]glid.GLID, numTiers)
	for i := range numTiers {
		tiers[i] = glid.New()
	}

	for ti, tierID := range tiers {
		for ci := range chunksPerTier {
			var cid chunk.ChunkID
			cid[0] = byte(ti)
			cid[1] = byte(ci)
			cmd := MarshalVaultChunkCommand(tierID, tierfsm.MarshalCreateChunk(cid, now, now, now))
			if r := src.Apply(&hraft.Log{Data: cmd}); r != nil {
				t.Fatalf("apply tier=%d chunk=%d: %v", ti, ci, r)
			}
		}
	}

	expected := vaultFSMFingerprint(src)

	// Persist snapshot bytes.
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf fingerprintBuilderBytes
	sink := &bufSink{Writer: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Restore into a fresh FSM from those bytes.
	dst := NewFSM()
	if err := dst.Restore(io.NopCloser(&readBytesCloser{data: buf.Bytes()})); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if got := vaultFSMFingerprint(dst); got != expected {
		t.Fatalf("round-trip fingerprint mismatch.\nexpected:\n%s\ngot:\n%s", expected, got)
	}

	// Sanity: total entry count matches.
	total := 0
	for _, tierID := range tiers {
		if sub := dst.TierFSM(tierID); sub != nil {
			total += len(sub.List())
		}
	}
	if want := numTiers * chunksPerTier; total != want {
		t.Errorf("expected %d total entries, got %d", want, total)
	}
}

// fingerprintBuilderBytes is a minimal io.Writer-backed byte buffer that
// does not pull in bytes.Buffer purely for this one test. Satisfies the
// parts of the interface the snapshot persist path needs.
type fingerprintBuilderBytes struct {
	buf []byte
}

func (b *fingerprintBuilderBytes) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}
func (b *fingerprintBuilderBytes) Bytes() []byte { return b.buf }

type readBytesCloser struct {
	data []byte
	off  int
}

func (r *readBytesCloser) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}
func (r *readBytesCloser) Close() error { return nil }

// --- Test helpers ---

// applyWithLeaderRetry submits cmd to whichever node is currently leader,
// re-resolving leadership and retrying on transient failures (hraft
// returns ErrLeadershipLost / ErrNotLeader / ErrLeadershipTransferInProgress
// when a command races an election). These errors mean the command did NOT
// commit — the caller must replay.
//
// Used by the concurrent-writes scenario where leader flap under contention
// is expected; production code has the same retry shape in
// cluster.VaultCtlTierApplyForwarder.
func applyWithLeaderRetry(h *reliabilityHarness, cmd []byte, maxAttempts int, timeout time.Duration) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		leaderID := h.waitForLeader()
		h.nodes[leaderID].mu.Lock()
		r := h.nodes[leaderID].raft
		h.nodes[leaderID].mu.Unlock()
		if r == nil {
			lastErr = fmt.Errorf("attempt %d: leader %s disappeared", attempt, leaderID)
			time.Sleep(20 * time.Millisecond)
			continue
		}
		err := r.Apply(cmd, timeout).Error()
		if err == nil {
			return nil
		}
		if !isTransientLeaderErr(err) {
			return fmt.Errorf("attempt %d: %w", attempt, err)
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

// isTransientLeaderErr reports whether err is one of hraft's retryable
// leadership-flap conditions. Apply returns these when a command races an
// election; the command did NOT commit and the caller must retry.
func isTransientLeaderErr(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, hraft.ErrLeadershipLost),
		errors.Is(err, hraft.ErrNotLeader),
		errors.Is(err, hraft.ErrLeadershipTransferInProgress):
		return true
	default:
		return false
	}
}

// removeDirContents deletes every entry inside dir (but keeps dir itself).
// Used by the follower-wipe scenario to simulate a disk failure / fresh
// replacement on an existing voter.
func removeDirContents(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func chunkIDWithPrefix(b byte) chunk.ChunkID {
	var id chunk.ChunkID
	id[0] = b
	return id
}

// assertSubsetConverged polls until every named node's FSM fingerprint
// matches, with AppliedIndex caught up to the leader's LastIndex. Used
// when part of the cluster is stopped and the full-cluster assertion
// would hang.
func assertSubsetConverged(t *testing.T, h *reliabilityHarness, ids []string) {
	t.Helper()
	if len(ids) == 0 {
		t.Fatal("subset is empty")
	}
	deadline := time.Now().Add(harnessConvergeWait)
	var lastPrints map[string]string
	for time.Now().Before(deadline) {
		// Find the leader among the subset.
		var leaderLast uint64
		leaderPrint := ""
		leaderID := ""
		for _, id := range ids {
			n := h.nodes[id]
			n.mu.Lock()
			r := n.raft
			fsm := n.fsm
			n.mu.Unlock()
			if r != nil && r.State() == hraft.Leader {
				leaderID = id
				leaderLast = r.LastIndex()
				if r.AppliedIndex() < leaderLast {
					break // leader not caught up yet
				}
				if fsm != nil {
					leaderPrint = vaultFSMFingerprint(fsm)
				}
				break
			}
		}
		if leaderID == "" || leaderPrint == "" && leaderLast > 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		lastPrints = map[string]string{leaderID: leaderPrint}
		allMatch := true
		for _, id := range ids {
			if id == leaderID {
				continue
			}
			n := h.nodes[id]
			n.mu.Lock()
			r := n.raft
			fsm := n.fsm
			n.mu.Unlock()
			if r == nil || fsm == nil {
				allMatch = false
				continue
			}
			if r.AppliedIndex() < leaderLast {
				allMatch = false
				lastPrints[id] = fmt.Sprintf("<behind: applied=%d leaderLast=%d>",
					r.AppliedIndex(), leaderLast)
				continue
			}
			p := vaultFSMFingerprint(fsm)
			lastPrints[id] = p
			if p != leaderPrint {
				allMatch = false
			}
		}
		if allMatch && len(lastPrints) == len(ids) {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("subset did not converge within %s:\n%s",
		harnessConvergeWait, formatPrints(lastPrints))
}

// Compile-time check: hraft.Raft exposes the methods the harness depends
// on. Keeps future hraft upgrades from silently removing what we rely on.
var _ = func(r *hraft.Raft) {
	_ = r.State()
	_ = r.AppliedIndex()
}
