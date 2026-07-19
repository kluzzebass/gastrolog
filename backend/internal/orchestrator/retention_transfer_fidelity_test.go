package orchestrator

// gastrolog-2l918 review finding 5: ensureDestManifestEntry used to
// rebuild the announced manifest entry from chunk.ChunkMeta
// (chunkMetaToManifestEntry), which drops Hash / KeyScheme /
// IngestTSMonotonic / the GLCB section-offset fields — chunk.ChunkMeta
// simply doesn't carry them (chunk/meta.go / ToChunkMeta). The fix
// (sourceManifestEntryForTransfer) copies the SOURCE vault-ctl FSM's own
// entry instead, only overriding the fields transfer legitimately changes
// (SealedAt, TransferSourceVaultID, Holders, RetentionPending).
//
// This needs a REAL vault-ctl FSM handle for the source vault (only
// orch.vaultCtlHandle resolves one), so it stands up a single-node raft
// group the same way after_vault_ctl_restore_test.go does — reusing that
// file's waitForRaftLeader / afterRestoreRaftTimeouts / bufSize helpers
// (same package).

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// newSingleNodeVaultCtlOrch stands up a real, single-voter vault-ctl raft
// group for vaultID and returns an Orchestrator wired to it (groupMgr set,
// nothing else) plus the raft group itself (to Apply seed commands
// directly, exactly like TestCreateGroupRestoreWithInstanceDoesNotDeadlock
// does). Callers get orch.vaultCtlHandle(vaultID) and
// orch.ApplyVaultControlPlane(vaultID, ...) working for real.
func newSingleNodeVaultCtlOrch(t *testing.T, vaultID glid.GLID) (*Orchestrator, *raftgroup.Group) {
	t.Helper()
	const nodeID = "node-fidelity"
	lis := bufconn.Listen(afterRestoreTestBufSize)
	srv := grpc.NewServer()
	tp := multiraft.New(
		hraft.ServerAddress(nodeID),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	pool := multiraft.NewSimpleDialerPeerPool(map[string]func() (net.Conn, error){
		nodeID: func() (net.Conn, error) { return lis.Dial() },
	})
	tp.SetPeerConnPool(pool)
	tp.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		pool.Close()
		srv.Stop()
		_ = tp.Close()
	})

	baseDir := t.TempDir()
	wal, err := raftwal.Open(filepath.Join(baseDir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })

	mgr := raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{
		Transport: tp,
		NodeID:    nodeID,
		BaseDir:   baseDir,
		WAL:       wal,
	})
	t.Cleanup(func() { mgr.Shutdown() })

	groupID := raftgroup.VaultControlPlaneGroupID(vaultID)
	hb, el, ll := afterRestoreRaftTimeouts()
	seed := []hraft.Server{{ID: hraft.ServerID(nodeID), Address: hraft.ServerAddress(nodeID)}}
	g, err := mgr.CreateGroup(raftgroup.GroupConfig{
		GroupID:            groupID,
		FSM:                vaultraft.NewFSM(),
		SeedMembers:        seed,
		HeartbeatTimeout:   hb,
		ElectionTimeout:    el,
		LeaderLeaseTimeout: ll,
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	waitForRaftLeader(t, g, 5*time.Second)

	orch, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.groupMgr = mgr
	return orch, g
}

// TestSourceManifestEntryForTransferPreservesFidelityFields is the direct
// pin for finding 5: with a real source FSM handle available,
// sourceManifestEntryForTransfer must return the SOURCE's own entry —
// Hash, KeyScheme, and IngestTSMonotonic all round-trip — not a
// chunkMetaToManifestEntry rebuild that zeroes them.
func TestSourceManifestEntryForTransferPreservesFidelityFields(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch, g := newSingleNodeVaultCtlOrch(t, vaultID)

	id := chunk.ChunkID(glid.New())
	now := time.Now().UTC()
	apply := func(data []byte) {
		t.Helper()
		if err := g.Raft.Apply(vaultraft.MarshalVaultChunkCommand(vaultID, data), 5*time.Second).Error(); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	apply(vaultctlfsm.MarshalCreateChunk(id, now, now, now))
	// ingestTSMonotonic=true: the field chunkMetaToManifestEntry drops.
	apply(vaultctlfsm.MarshalSealChunk(id, now, 5, 500, now, now, now, true, now))
	var hash [32]byte
	hash[0], hash[31] = 0xAB, 0xCD
	cloudSvc := glid.New()
	apply(vaultctlfsm.MarshalUploadChunk(id, 500, 10, 20, 30, 40, hash, cloudSvc, 3))

	r := &retentionRunner{vaultID: vaultID, orch: orch, now: time.Now}
	// meta deliberately carries NEITHER Hash nor KeyScheme — chunk.ChunkMeta
	// has no such fields at all (see chunk.ChunkMeta / ToChunkMeta), so a
	// ChunkMeta-only rebuild has no way to recover them from this input.
	meta := chunk.ChunkMeta{ID: id, RecordCount: 5, Sealed: true, IngestTSMonotonic: true}

	got := r.sourceManifestEntryForTransfer(id, meta)
	if got.Hash != hash {
		t.Errorf("Hash = %x, want %x (source FSM entry must be copied, not rebuilt from ChunkMeta)", got.Hash, hash)
	}
	if got.KeyScheme != 3 {
		t.Errorf("KeyScheme = %d, want 3", got.KeyScheme)
	}
	if !got.IngestTSMonotonic {
		t.Error("IngestTSMonotonic = false, want true (chunkMetaToManifestEntry drops this field)")
	}
	if got.IngestIdxOffset != 10 || got.IngestIdxSize != 20 || got.SourceIdxOffset != 30 || got.SourceIdxSize != 40 {
		t.Errorf("index offsets not preserved: %+v", got)
	}
	if got.CloudServiceID != cloudSvc {
		t.Errorf("CloudServiceID = %v, want %v", got.CloudServiceID, cloudSvc)
	}
}

// TestSourceManifestEntryForTransferFallsBackWithoutOrchestrator pins the
// bare-test-harness fallback: with no orchestrator (no local source FSM
// handle reachable), sourceManifestEntryForTransfer degrades to
// chunkMetaToManifestEntry — same as before this fix — rather than
// panicking or failing the transfer outright.
func TestSourceManifestEntryForTransferFallsBackWithoutOrchestrator(t *testing.T) {
	t.Parallel()
	r := &retentionRunner{vaultID: glid.New(), now: time.Now}
	id := chunk.NewChunkID()
	meta := chunk.ChunkMeta{ID: id, RecordCount: 9, Sealed: true, IngestTSMonotonic: true}

	got := r.sourceManifestEntryForTransfer(id, meta)
	if got.RecordCount != 9 {
		t.Errorf("RecordCount = %d, want 9 (fallback must still carry the basics)", got.RecordCount)
	}
	if got.IngestTSMonotonic {
		t.Error("want IngestTSMonotonic false: the ChunkMeta-rebuild fallback (chunkMetaToManifestEntry) is documented to drop it")
	}
}
