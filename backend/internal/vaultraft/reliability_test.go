package vaultraft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	tierfsm "gastrolog/internal/tier/raftfsm"

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
//   - FollowerWipe_CatchupViaSnapshotOrReplay  (new-ish follower catches up)

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
	cmd := MarshalTierCommand(tierID, tierfsm.MarshalCreateChunk(chunkIDWithPrefix(0x11), now, now, now))
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
				cmd := MarshalTierCommand(tierID, wire)
				// Always route through the leader. Re-resolve each time because
				// leadership may flap under contention.
				leaderID := h.waitForLeader()
				n := h.nodes[leaderID]
				n.mu.Lock()
				r := n.raft
				n.mu.Unlock()
				if r == nil {
					errCh <- fmt.Errorf("writer %d: leader disappeared", writerIdx)
					return
				}
				if err := r.Apply(cmd, 3*time.Second).Error(); err != nil {
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

// --- Test helpers ---

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
