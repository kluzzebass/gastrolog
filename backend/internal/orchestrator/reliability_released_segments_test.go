package orchestrator_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// releasedSegmentState is one node's view of the released-segment tombstones:
// the release sequence, the tombstone count, and the serialised
// released-segment section of its snapshot (sequence, tombstones, purge
// confirmations) for comparing nodes.
type releasedSegmentState struct {
	releaseSeq uint64
	tombstones int
	raw        []byte
}

func (h *orchRelHarness) releasedSegmentState(v vaultSpec, nodeID string) (releasedSegmentState, bool) {
	sub := h.vaultCtlSubFSM(v, nodeID)
	if sub == nil {
		return releasedSegmentState{}, false
	}
	snap := sub.SnapshotProto()
	raw, err := proto.Marshal(&gastrologv1.VaultCtlSnapshot{
		ReleaseSeq:                snap.GetReleaseSeq(),
		ReleasedSegments:          snap.GetReleasedSegments(),
		SegmentPurgeConfirmations: snap.GetSegmentPurgeConfirmations(),
	})
	if err != nil {
		h.t.Fatalf("marshal snapshot on %s: %v", h.nodes[nodeID].label, err)
	}
	return releasedSegmentState{
		releaseSeq: snap.GetReleaseSeq(),
		tombstones: len(snap.GetReleasedSegments()),
		raw:        raw,
	}, true
}

// waitTombstonesPruned waits until every given node's FSM has applied at least
// one release and holds no released-segment tombstone.
func (h *orchRelHarness) waitTombstonesPruned(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: released-segment tombstones pruned", v.label), 50*time.Millisecond, func() (string, bool) {
		views := map[string]string{}
		done := true
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			st, ok := h.releasedSegmentState(v, nodeID)
			if !ok {
				views[h.nodes[nodeID].label] = "no-fsm"
				done = false
				continue
			}
			views[h.nodes[nodeID].label] = fmt.Sprintf("seq=%d tombstones=%d", st.releaseSeq, st.tombstones)
			if st.releaseSeq == 0 || st.tombstones != 0 {
				done = false
			}
		}
		return fmt.Sprintf("%v", views), done
	}, func() { h.dumpPipelineState(v) })
}

// waitPurgeConfirmed waits until each given node's purge confirmation, as
// recorded in the vault-ctl FSM, has reached the release sequence.
func (h *orchRelHarness) waitPurgeConfirmed(v vaultSpec, observer string, nodeIdxs []int) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: purge confirmations reach the release sequence", v.label), 50*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, observer)
		if sub == nil {
			return "no-fsm", false
		}
		seq := sub.ReleaseSeq()
		views := map[string]uint64{}
		done := seq > 0
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			c := sub.SegmentPurgeConfirmed(nodeID)
			views[h.nodes[nodeID].label] = c
			if c != seq {
				done = false
			}
		}
		return fmt.Sprintf("seq=%d confirmed=%v", seq, views), done
	}, func() { h.dumpPipelineState(v) })
}

// assertReleasedStateIdentical waits until every given node's released-segment
// snapshot section is byte-identical — tombstones, release sequence, and
// confirmations agree.
func (h *orchRelHarness) assertReleasedStateIdentical(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: identical vault-ctl snapshots", v.label), 50*time.Millisecond, func() (string, bool) {
		var first []byte
		same := true
		sizes := map[string]int{}
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			st, ok := h.releasedSegmentState(v, nodeID)
			if !ok {
				return "no-fsm", false
			}
			sizes[h.nodes[nodeID].label] = len(st.raw)
			if first == nil {
				first = st.raw
			} else if !bytes.Equal(first, st.raw) {
				same = false
			}
		}
		return fmt.Sprintf("sizes=%v", sizes), same
	}, func() { h.dumpPipelineState(v) })
}

func (h *orchRelHarness) vaultCtlLeaderAmong(v vaultSpec) *orchRelNode {
	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if n == nil || n.groupMgr == nil {
			continue
		}
		if g := n.groupMgr.GetGroup(gid); g != nil && g.Raft.State() == hraft.Leader {
			return n
		}
	}
	return nil
}

// Every node is an origin for every vault, so a node outside the placement
// still holds its released segments in completed/. Once the homes chunk and
// release them, every node purges its staging copies and confirms, and the
// vault-ctl leader prunes the tombstones on every node.
func TestOrchPipeline_ReleasedSegmentTombstonesPruneOnEveryNode(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	all := []int{0, 1, 2, 3}
	origin := h.nodeIDs[3]

	h.submitIngestRecords(origin, 2*pipelineChunkMaxRecords, "released")
	h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	h.waitRegistryDrained(v, all)
	h.waitTombstonesPruned(v, all)
	h.assertReleasedStateIdentical(v, all)

	ids, err := paths.ListSegmentIDs(paths.CompletedDir(h.pipelineVaultRoot(origin, v)))
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("list origin completed/: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("non-home origin still holds %d released segments in completed/", len(ids))
	}
}

// memberDownRelease is a 4-node cluster (vault on nodes 1-3, ingest on node 4)
// whose node 3 was down while every ingested segment was released, with a
// head/ copy of one released segment planted on node 3 as the copy it missed
// purging. Every live node has confirmed the whole release sequence.
type memberDownRelease struct {
	h        *orchRelHarness
	v        vaultSpec
	down     string
	observer string
	held     int
	planted  string
}

func releaseWithMemberDown(t *testing.T) *memberDownRelease {
	t.Helper()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	m := &memberDownRelease{h: h, v: h.vaults[1], down: h.nodeIDs[2], observer: h.nodeIDs[0]}
	live := []int{0, 1, 3}

	h.stopNode(m.down)
	h.submitIngestRecords(h.nodeIDs[3], 2*pipelineChunkMaxRecords, "while-down")
	h.waitSealedRecords(m.v, m.observer, 2*pipelineChunkMaxRecords)
	h.waitRegistryDrained(m.v, live)
	h.waitPurgeConfirmed(m.v, m.observer, live)

	sub := h.vaultCtlSubFSM(m.v, m.observer)
	m.held = sub.ReleasedSegmentCount()
	if m.held == 0 {
		t.Fatal("no released-segment tombstones while a member is down")
	}
	missed := glid.FromBytes(sub.SnapshotProto().GetReleasedSegments()[0].GetSegmentId())
	root := h.pipelineVaultRoot(m.down, m.v)
	if err := os.MkdirAll(paths.HeadDir(root), 0o755); err != nil {
		t.Fatalf("mkdir head/: %v", err)
	}
	m.planted = paths.HeadSegment(root, missed)
	if err := os.WriteFile(m.planted, []byte("missed-release"), 0o644); err != nil {
		t.Fatalf("plant head/ copy: %v", err)
	}
	return m
}

// tombstonesHeld is the invariant that no node has pruned a tombstone.
func (m *memberDownRelease) tombstonesHeld() (string, bool) {
	for _, id := range m.h.nodeIDs {
		st, ok := m.h.releasedSegmentState(m.v, id)
		if ok && st.tombstones < m.held {
			return fmt.Sprintf("%s pruned to %d of %d tombstones before %s confirmed",
				m.h.nodes[id].label, st.tombstones, m.held, m.h.nodes[m.down].label), false
		}
	}
	return "", true
}

// A member that is down when segments are released holds their tombstones:
// it may still have the segments' bytes and would republish them on restart.
// Vault-ctl leadership moving in the meantime changes nothing — the new
// leader prunes from the same replicated confirmations once the member is
// back, has purged what it missed, and has confirmed.
func TestOrchPipeline_DownMemberHoldsReleasedSegmentTombstonesAcrossLeaderChange(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	m := releaseWithMemberDown(t)
	h, v := m.h, m.v

	h.holdAcrossSweeps("tombstones held by the down member", m.observer, catchupSweepJob, 3, m.tombstonesHeld)

	before := h.vaultCtlLeaderAmong(v)
	if before == nil {
		t.Fatal("no vault-ctl leader")
	}
	g := before.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(v.id))
	if err := g.Raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("leadership transfer from %s: %v", before.label, err)
	}
	h.waitProgress("vault-ctl leadership moves", 30*time.Millisecond, func() (string, bool) {
		after := h.vaultCtlLeaderAmong(v)
		if after == nil {
			return "electing", false
		}
		return after.label, after.id != before.id
	}, nil)
	h.holdAcrossSweeps("tombstones held across the leader change", m.observer, catchupSweepJob, 3, m.tombstonesHeld)

	h.startNode(m.down)
	h.waitForAllReady()
	h.waitTombstonesPruned(v, []int{0, 1, 2, 3})
	h.assertReleasedStateIdentical(v, []int{0, 1, 2, 3})
	if _, err := os.Stat(m.planted); !os.IsNotExist(err) {
		t.Fatalf("returning member kept its staging copy of a released segment (stat err=%v)", err)
	}
}

// A returning member that cannot delete its copy of a released segment does
// not confirm, so the tombstone stays — and keeps refusing that member's
// restart republish — until the purge succeeds.
func TestOrchPipeline_UnpurgeableStagingCopyHoldsReleasedSegmentTombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	m := releaseWithMemberDown(t)
	h, v := m.h, m.v

	headDir := paths.HeadDir(h.pipelineVaultRoot(m.down, v))
	if err := os.Chmod(headDir, 0o555); err != nil {
		t.Fatalf("chmod head/: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(headDir, 0o755) })

	h.startNode(m.down)
	h.waitForAllReady()
	h.holdAcrossSweeps("tombstones held while the returning member cannot purge", m.down, catchupSweepJob, 3, func() (string, bool) {
		if desc, ok := m.tombstonesHeld(); !ok {
			return desc, false
		}
		if _, err := os.Stat(m.planted); err != nil {
			return fmt.Sprintf("planted copy gone from a read-only head/: %v", err), false
		}
		return "", true
	})

	if err := os.Chmod(headDir, 0o755); err != nil {
		t.Fatalf("chmod head/ back: %v", err)
	}
	h.waitTombstonesPruned(v, []int{0, 1, 2, 3})
	if _, err := os.Stat(m.planted); !os.IsNotExist(err) {
		t.Fatalf("staging copy survived once purgeable (stat err=%v)", err)
	}
}
