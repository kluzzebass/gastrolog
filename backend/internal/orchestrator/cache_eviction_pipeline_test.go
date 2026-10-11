package orchestrator

import (
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// pipelineCloudHome is one home of a cloud-backed pipeline vault, wired the
// way vault construction wires it: the lazy pipeline-GLCB resolver and
// locator, the FSM-grounded cloud-backed resolver, holder receipts that
// commit into the vault-ctl FSM, and a lifecycle reconciler.
type pipelineCloudHome struct {
	orch      *Orchestrator
	vaultID   glid.GLID
	nodeID    string
	fsm       *vaultctlfsm.FSM
	cm        *chunkfile.Manager
	chunkRoot string
}

func newPipelineCloudHome(t *testing.T, budget uint64) *pipelineCloudHome {
	t.Helper()
	h := &pipelineCloudHome{vaultID: glid.New(), nodeID: "node-A", fsm: vaultctlfsm.New()}
	h.orch = newTestOrch(t, Config{LocalNodeID: h.nodeID})
	markPipelineIngestVault(t, h.orch, h.vaultID, true)
	h.chunkRoot = filepath.Join(h.orch.segmentsDir, h.vaultID.String(), "chunks")

	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:              t.TempDir(),
		Now:              time.Now,
		RotationPolicy:   chunk.NewRecordCountPolicy(1000),
		CloudStore:       blobstore.NewMemory(),
		VaultID:          h.vaultID,
		CacheEviction:    "lru",
		CacheBudgetBytes: budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	h.cm = cm

	applyFSM := func(data []byte, err error) error {
		if err != nil {
			return err
		}
		if resp := h.fsm.Apply(&hraft.Log{Data: data}); resp != nil {
			if rerr, ok := resp.(error); ok {
				return rerr
			}
		}
		return nil
	}
	vaultInst := &VaultInstance{
		VaultID: h.vaultID,
		Type:    "file",
		Chunks:  cm,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftAckChunkHolders: func(ids []chunk.ChunkID, nodeID string) error {
				return applyFSM(vaultctlfsm.MarshalAckChunkHolders(ids, nodeID))
			},
			ApplyRaftRevokeChunkHolders: func(ids []chunk.ChunkID, nodeID string) error {
				return applyFSM(vaultctlfsm.MarshalRevokeChunkHolders(ids, nodeID))
			},
		},
	}
	installGLCBResolverOn(vaultInst, func() (*vaultctlfsm.FSM, string, bool) { return h.fsm, h.chunkRoot, true })
	cm.SetCloudBackedChunkResolver(func(id chunk.ChunkID) (chunk.CloudBackedChunkInfo, bool) {
		e := h.fsm.Get(id)
		if e == nil || !e.CloudBacked {
			return chunk.CloudBackedChunkInfo{}, false
		}
		return cloudBackedInfoFromEntry(*e), true
	})
	cm.SetCloudBackedChunkLister(func() []chunk.ChunkID {
		var ids []chunk.ChunkID
		for _, e := range h.fsm.List() {
			if e.CloudBacked {
				ids = append(ids, e.ID)
			}
		}
		return ids
	})
	rec := NewVaultLifecycleReconciler(h.orch, h.vaultID, vaultInst, h.nodeID, slog.Default())
	rec.Wire(h.fsm)
	vaultInst.Reconciler = rec
	h.orch.RegisterVault(NewVault(h.vaultID, vaultInst))
	return h
}

func (h *pipelineCloudHome) apply(t *testing.T, data []byte) {
	t.Helper()
	if resp := h.fsm.Apply(&hraft.Log{Data: data}); resp != nil {
		if err, ok := resp.(error); ok {
			t.Fatalf("fsm apply: %v", err)
		}
	}
}

// buildSealedChunk writes the home's pipeline GLCB for a new chunk, commits
// the chunk's seal, and commits this home's holder receipt — the state right
// after a home builds a sealed chunk.
func (h *pipelineCloudHome) buildSealedChunk(t *testing.T, records int) (chunk.ChunkID, string) {
	t.Helper()
	id := chunk.NewChunkID()
	base := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	path, info := glcbtest.WriteSealedBlob(t, h.chunkRoot, id, h.vaultID,
		glcbtest.Records(records, base, glid.New(), "pipeline-cloud-evict"))
	h.apply(t, vaultctlfsm.MarshalCreateChunk(id, info.WriteStart, info.IngestStart, info.SourceStart))
	h.apply(t, vaultctlfsm.MarshalSealChunk(id, info.WriteEnd, info.RecordCount, info.Bytes,
		info.IngestStart, info.IngestEnd, info.SourceEnd, info.IngestTSMonotonic, time.Now()))
	ack, err := vaultctlfsm.MarshalAckChunkHolders([]chunk.ChunkID{id}, h.nodeID)
	if err != nil {
		t.Fatal(err)
	}
	h.apply(t, ack)
	return id, path
}

// upload puts the chunk's object in the cloud store. commit additionally
// commits CmdUploadChunk — without it the object exists but the cluster has
// not recorded the upload.
func (h *pipelineCloudHome) upload(t *testing.T, id chunk.ChunkID, commit bool) {
	t.Helper()
	if err := h.cm.UploadToCloud(id); err != nil {
		t.Fatalf("UploadToCloud: %v", err)
	}
	if commit {
		h.apply(t, vaultctlfsm.MarshalUploadChunk(id, 1, 0, 0, 0, 0, [32]byte{}, glid.New(), 0))
	}
}

func (h *pipelineCloudHome) holds(id chunk.ChunkID) bool {
	e := h.fsm.Get(id)
	return e != nil && slices.Contains(e.Holders, h.nodeID)
}

func readChunkRecords(t *testing.T, cm chunk.ChunkManager, id chunk.ChunkID) int {
	t.Helper()
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor(%s): %v", id, err)
	}
	defer func() { _ = cursor.Close() }()
	n := 0
	for {
		if _, _, err := cursor.Next(); err != nil {
			if err == chunk.ErrNoMoreRecords {
				return n
			}
			t.Fatalf("cursor.Next: %v", err)
		}
		n++
	}
}

// TestCacheEvictionSweep_PipelineGLCBEvictedAndReceiptWithdrawn: the scheduled
// sweep evicts a cloud-backed chunk's pipeline GLCB under budget pressure, the
// home's holder receipt is withdrawn on that same edge (residency must not
// claim bytes the home no longer has), and the chunk still reads in full
// through the cloud.
func TestCacheEvictionSweep_PipelineGLCBEvictedAndReceiptWithdrawn(t *testing.T) {
	t.Parallel()
	h := newPipelineCloudHome(t, 1)
	id, path := h.buildSealedChunk(t, 30)
	h.upload(t, id, true)
	if !h.holds(id) {
		t.Fatal("precondition: home holds the chunk before eviction")
	}

	h.orch.cacheEvictionSweepAll()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("pipeline GLCB survived the eviction sweep: stat err=%v", err)
	}
	if h.holds(id) {
		t.Fatal("holder receipt still claims the evicted chunk after the sweep")
	}
	if got := readChunkRecords(t, h.cm, id); got != 30 {
		t.Fatalf("read after eviction served %d records, want 30", got)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("re-fetch landed in the pipeline chunk root, which would re-earn a holder receipt for a cache copy")
	}
}

// TestCacheEvictionSweep_UncommittedUploadKeepsPipelineGLCB: the object is in
// the store but CmdUploadChunk has not committed. Eviction must leave the GLCB
// and the receipt alone — to the cluster this chunk is not yet cloud-backed.
func TestCacheEvictionSweep_UncommittedUploadKeepsPipelineGLCB(t *testing.T) {
	t.Parallel()
	h := newPipelineCloudHome(t, 1)
	id, path := h.buildSealedChunk(t, 20)
	h.upload(t, id, false)

	h.orch.cacheEvictionSweepAll()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pipeline GLCB of an uncommitted upload was evicted: %v", err)
	}
	if !h.holds(id) {
		t.Fatal("holder receipt withdrawn for a chunk whose bytes are still here")
	}
}

// TestReconcileTick_ReceiptTruthForCloudBackedChunks: the periodic reconcile is
// the backstop for the eviction edge — it withdraws a cloud-backed chunk's
// receipt when the pipeline GLCB is gone and earns it when the GLCB is
// present, and never pulls a cloud-backed chunk back.
func TestReconcileTick_ReceiptTruthForCloudBackedChunks(t *testing.T) {
	t.Parallel()
	h := newPipelineCloudHome(t, 0)
	gone, gonePath := h.buildSealedChunk(t, 10)
	h.upload(t, gone, true)
	kept, _ := h.buildSealedChunk(t, 10)
	h.upload(t, kept, true)
	revoke, err := vaultctlfsm.MarshalRevokeChunkHolders([]chunk.ChunkID{kept}, h.nodeID)
	if err != nil {
		t.Fatal(err)
	}
	h.apply(t, revoke)
	if err := os.RemoveAll(filepath.Dir(gonePath)); err != nil {
		t.Fatal(err)
	}

	h.orch.vaults[h.vaultID].Instance.Reconciler.ReconcileTick()

	if h.holds(gone) {
		t.Error("receipt kept for a cloud-backed chunk whose pipeline GLCB is gone")
	}
	if !h.holds(kept) {
		t.Error("receipt not earned for a cloud-backed chunk whose pipeline GLCB is present")
	}
	if _, err := os.Stat(chunking.ChunkGLCBPath(h.chunkRoot, gone)); !os.IsNotExist(err) {
		t.Error("reconcile pulled a cloud-backed chunk back into the chunk root")
	}
}
