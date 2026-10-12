package orchestrator_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"

	"google.golang.org/protobuf/proto"
)

// chunkTombstoneState is one node's view of the chunk tombstones: the delete
// sequence, the tombstone count, and the serialised chunk-tombstone section of
// its snapshot (sequence, tombstones, chunk purge confirmations) for comparing
// nodes.
type chunkTombstoneState struct {
	deleteSeq  uint64
	tombstones int
	raw        []byte
}

func (h *orchRelHarness) chunkTombstoneState(v vaultSpec, nodeID string) (chunkTombstoneState, bool) {
	sub := h.vaultCtlSubFSM(v, nodeID)
	if sub == nil {
		return chunkTombstoneState{}, false
	}
	snap := sub.SnapshotProto()
	raw, err := proto.Marshal(&gastrologv1.VaultCtlSnapshot{
		DeleteSeq:               snap.GetDeleteSeq(),
		Tombstones:              snap.GetTombstones(),
		ChunkPurgeConfirmations: snap.GetChunkPurgeConfirmations(),
	})
	if err != nil {
		h.t.Fatalf("marshal snapshot on %s: %v", h.nodes[nodeID].label, err)
	}
	return chunkTombstoneState{
		deleteSeq:  snap.GetDeleteSeq(),
		tombstones: len(snap.GetTombstones()),
		raw:        raw,
	}, true
}

// configureDeleteRetention gives a vault a retention rule every sealed chunk
// is already past, with the delete disposition.
func configureDeleteRetention(t *testing.T, h *orchRelHarness, v vaultSpec) {
	t.Helper()
	policyID := putTransferRetentionPolicy(t, h, "delete-everything")
	ctx := context.Background()
	cfg, err := h.cfgStore.GetVault(ctx, v.id)
	if err != nil || cfg == nil {
		t.Fatalf("GetVault %s: %v", v.label, err)
	}
	cfg.RetentionDisposition = system.RetentionDispositionDelete
	cfg.RetentionTransferTargetVaultID = nil
	cfg.RetentionRules = []system.RetentionRule{{RetentionPolicyID: policyID}}
	if err := h.cfgStore.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault %s (delete retention): %v", v.label, err)
	}
}

// waitChunksDeleted waits until the vault has deleted every sealed chunk.
func (h *orchRelHarness) waitChunksDeleted(v vaultSpec, observer string) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: retention deletes every sealed chunk", v.label), 50*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, observer)
		if sub == nil {
			return "no-fsm", false
		}
		n := len(h.sealedPipelineChunks(v, observer))
		return fmt.Sprintf("sealed=%d pending=%d delete_seq=%d", n, len(sub.PendingDeleteIDs()), sub.DeleteSeq()),
			n == 0 && len(sub.PendingDeleteIDs()) == 0 && sub.DeleteSeq() > 0
	}, func() { h.dumpPipelineState(v) })
}

// waitChunkTombstonesPruned waits until every given node's FSM has tombstoned
// at least one chunk and holds no chunk tombstone.
func (h *orchRelHarness) waitChunkTombstonesPruned(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: chunk tombstones pruned", v.label), 50*time.Millisecond, func() (string, bool) {
		views := map[string]string{}
		done := true
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			st, ok := h.chunkTombstoneState(v, nodeID)
			if !ok {
				views[h.nodes[nodeID].label] = "no-fsm"
				done = false
				continue
			}
			views[h.nodes[nodeID].label] = fmt.Sprintf("seq=%d tombstones=%d", st.deleteSeq, st.tombstones)
			if st.deleteSeq == 0 || st.tombstones != 0 {
				done = false
			}
		}
		return fmt.Sprintf("%v", views), done
	}, func() { h.dumpPipelineState(v) })
}

// waitChunkPurgeConfirmed waits until each given node's chunk purge
// confirmation, as recorded in the vault-ctl FSM, has reached the delete
// sequence.
func (h *orchRelHarness) waitChunkPurgeConfirmed(v vaultSpec, observer string, nodeIdxs []int) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: chunk purge confirmations reach the delete sequence", v.label), 50*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, observer)
		if sub == nil {
			return "no-fsm", false
		}
		seq := sub.DeleteSeq()
		views := map[string]uint64{}
		done := seq > 0
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			c := sub.ChunkPurgeConfirmed(nodeID)
			views[h.nodes[nodeID].label] = c
			if c != seq {
				done = false
			}
		}
		return fmt.Sprintf("seq=%d confirmed=%v", seq, views), done
	}, func() { h.dumpPipelineState(v) })
}

// assertChunkTombstoneStateIdentical waits until every given node's
// chunk-tombstone snapshot section is byte-identical.
func (h *orchRelHarness) assertChunkTombstoneStateIdentical(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: identical chunk tombstone state", v.label), 50*time.Millisecond, func() (string, bool) {
		var first []byte
		same := true
		views := map[string]uint64{}
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			st, ok := h.chunkTombstoneState(v, nodeID)
			if !ok {
				return "no-fsm", false
			}
			views[h.nodes[nodeID].label] = st.deleteSeq
			if first == nil {
				first = st.raw
			} else if !bytes.Equal(first, st.raw) {
				same = false
			}
		}
		return fmt.Sprintf("delete_seq=%v", views), same
	}, func() { h.dumpPipelineState(v) })
}

// plantChunkCopy writes a chunk staging dir for id on nodeID, as a copy of the
// chunk that node holds outside the delete's expected-acks set.
func (h *orchRelHarness) plantChunkCopy(nodeID string, v vaultSpec, id chunk.ChunkID) string {
	h.t.Helper()
	dir := filepath.Join(h.pipelineVaultRoot(nodeID, v), "chunks", id.String())
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.glcb"), []byte("planted"), 0o644); err != nil {
		h.t.Fatalf("plant chunk copy: %v", err)
	}
	return dir
}

// A node outside a vault's placement can still hold a chunk's bytes. Once
// retention deletes the chunks, every node purges its copies and confirms,
// and the vault-ctl leader prunes the chunk tombstones on every node — the
// same tombstone state everywhere.
func TestOrchPipeline_ChunkTombstonesPruneOnEveryNode(t *testing.T) {
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
	observer, outsider := h.nodeIDs[0], h.nodeIDs[3]

	h.submitIngestRecords(outsider, 2*pipelineChunkMaxRecords, "tombstoned")
	sealed := h.waitSealedRecords(v, observer, 2*pipelineChunkMaxRecords)
	var planted []string
	for _, e := range sealed {
		planted = append(planted, h.plantChunkCopy(outsider, v, e.ID))
	}

	configureDeleteRetention(t, h, v)
	h.waitChunksDeleted(v, observer)
	h.waitChunkTombstonesPruned(v, all)
	h.assertChunkTombstoneStateIdentical(v, all)
	for _, dir := range planted {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("node outside the placement kept its copy of a deleted chunk %s (stat err=%v)", dir, err)
		}
	}
}

// memberDownDelete is a 4-node cluster (vault on nodes 1-3) whose node 4 was
// down while retention deleted every chunk, with a copy of one deleted chunk
// planted on node 4 as the copy it missed purging. Every live node has
// confirmed the whole delete sequence.
type memberDownDelete struct {
	h        *orchRelHarness
	v        vaultSpec
	down     string
	observer string
	seq      uint64
	held     int
	planted  string
}

func deleteWithMemberDown(t *testing.T) *memberDownDelete {
	t.Helper()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	m := &memberDownDelete{h: h, v: h.vaults[1], down: h.nodeIDs[3], observer: h.nodeIDs[0]}

	h.submitIngestRecords(m.observer, 2*pipelineChunkMaxRecords, "deleted-while-down")
	h.waitSealedRecords(m.v, m.observer, 2*pipelineChunkMaxRecords)
	h.stopNode(m.down)
	configureDeleteRetention(t, h, m.v)
	h.waitChunksDeleted(m.v, m.observer)
	h.waitChunkPurgeConfirmed(m.v, m.observer, []int{0, 1, 2})

	sub := h.vaultCtlSubFSM(m.v, m.observer)
	m.seq = sub.DeleteSeq()
	m.held = sub.ChunkTombstoneCount()
	if m.held == 0 {
		t.Fatal("no chunk tombstones while a member is down")
	}
	// The copy is of the first chunk deleted, so any confirmation the
	// returning member can send covers it.
	tombstones := sub.SnapshotProto().GetTombstones()
	first := tombstones[0]
	for _, ts := range tombstones {
		if ts.GetDeleteSeq() < first.GetDeleteSeq() {
			first = ts
		}
	}
	m.planted = h.plantChunkCopy(m.down, m.v, chunk.ChunkID(glid.FromBytes(first.GetChunkId())))
	return m
}

// chunkTombstonesHeld is the invariant that no node has pruned a tombstone.
// A returning node still replaying up to the deletes has not pruned anything.
func (m *memberDownDelete) chunkTombstonesHeld() (string, bool) {
	for _, id := range m.h.nodeIDs {
		st, ok := m.h.chunkTombstoneState(m.v, id)
		if ok && st.deleteSeq >= m.seq && st.tombstones < m.held {
			return fmt.Sprintf("%s pruned to %d of %d chunk tombstones before %s confirmed",
				m.h.nodes[id].label, st.tombstones, m.held, m.h.nodes[m.down].label), false
		}
	}
	return "", true
}

// A member that is down when chunks are deleted holds their tombstones: it
// may still have the chunks' bytes, and only the tombstone lets it delete
// them rather than keep them as unknown orphans. Once it is back, has purged
// what it missed, and has confirmed, the leader prunes on every node.
func TestOrchPipeline_DownMemberHoldsChunkTombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	m := deleteWithMemberDown(t)
	h, v := m.h, m.v

	h.holdAcrossSweeps("chunk tombstones held by the down member", m.observer, catchupSweepJob, 3, m.chunkTombstonesHeld)

	h.startNode(m.down)
	h.waitForAllReady()
	h.waitChunkTombstonesPruned(v, []int{0, 1, 2, 3})
	h.assertChunkTombstoneStateIdentical(v, []int{0, 1, 2, 3})
	if _, err := os.Stat(m.planted); !os.IsNotExist(err) {
		t.Fatalf("returning member kept its copy of a deleted chunk (stat err=%v)", err)
	}
}

// A returning member that cannot delete its copy of a deleted chunk does not
// confirm, so the tombstones stay until the purge succeeds.
func TestOrchPipeline_UndeletableChunkCopyHoldsChunkTombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	m := deleteWithMemberDown(t)
	h, v := m.h, m.v

	chunksDir := filepath.Dir(m.planted)
	if err := os.Chmod(chunksDir, 0o555); err != nil {
		t.Fatalf("chmod chunks/: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(chunksDir, 0o755) })

	h.startNode(m.down)
	h.waitForAllReady()
	h.holdAcrossSweeps("chunk tombstones held while the returning member cannot purge", m.down, catchupSweepJob, 3, func() (string, bool) {
		if desc, ok := m.chunkTombstonesHeld(); !ok {
			return desc, false
		}
		if c := h.vaultCtlSubFSM(v, m.observer).ChunkPurgeConfirmed(m.down); c >= m.seq {
			return fmt.Sprintf("%s confirmed delete sequence %d while holding a copy", h.nodes[m.down].label, c), false
		}
		if _, err := os.Stat(m.planted); err != nil {
			return fmt.Sprintf("planted copy gone from a read-only chunks/: %v", err), false
		}
		return "", true
	})

	if err := os.Chmod(chunksDir, 0o755); err != nil {
		t.Fatalf("chmod chunks/ back: %v", err)
	}
	h.waitChunkTombstonesPruned(v, []int{0, 1, 2, 3})
	if _, err := os.Stat(m.planted); !os.IsNotExist(err) {
		t.Fatalf("chunk copy survived once deletable (stat err=%v)", err)
	}
}
