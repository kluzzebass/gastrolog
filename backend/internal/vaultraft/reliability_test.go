package vaultraft

import (
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
// Scenario coverage today:
//   - FreshCluster_AppliedIndexNonZero     (regression for gastrolog-5j6eu readiness bug)
//   - FreshCluster_FSMsConvergeEmpty
//   - LeaderApply_ReplicatesToFollowers
//   - Restart_AllNodes_ChunkStateSurvives
//   - Failover_LeaderDown_NewLeaderElected
//   - Failover_FollowerDown_QuorumHolds
//   - Partition_MinorityBlocked_HealReconverges
//
// Planned (not yet landed; tracked in issue body for gastrolog-5ff7z):
//   - KillMidApply_WALReplayMatchesFollowers   (needs crash-point injection)
//   - ConcurrentWrites_NoDivergence             (load + convergence)
//   - Snapshot_InstallRestoresFollowerFromScratch

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

// --- Test helpers ---

func chunkIDWithPrefix(b byte) chunk.ChunkID {
	var id chunk.ChunkID
	id[0] = b
	return id
}

// assertSubsetConverged polls until every named node's FSM fingerprint
// matches the first node's, or times out. Used when part of the cluster is
// stopped and the full-cluster assertion would hang.
func assertSubsetConverged(t *testing.T, h *reliabilityHarness, ids []string) {
	t.Helper()
	if len(ids) == 0 {
		t.Fatal("subset is empty")
	}
	deadline := time.Now().Add(harnessConvergeWait)
	var lastPrints map[string]string
	for time.Now().Before(deadline) {
		lastPrints = map[string]string{}
		baselineID := ""
		for _, id := range ids {
			n := h.nodes[id]
			n.mu.Lock()
			fsm := n.fsm
			r := n.raft
			n.mu.Unlock()
			if fsm == nil || r == nil {
				continue
			}
			p := vaultFSMFingerprint(fsm)
			lastPrints[id] = p
			if baselineID == "" {
				baselineID = id
			}
		}
		if baselineID == "" {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		baseline := lastPrints[baselineID]
		allMatch := true
		for _, p := range lastPrints {
			if p != baseline {
				allMatch = false
				break
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
