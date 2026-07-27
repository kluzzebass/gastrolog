package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/diskusage"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/system"
	"gastrolog/internal/system/memory"
)

func TestPipelineSegmentByteAccountingPrefersFSM(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	segFSM := glid.New()
	segOrphan := glid.New()
	for _, spec := range []struct {
		id   glid.GLID
		dir  string
		body string
	}{
		{segFSM, paths.CompletedDir(dir), "x"},
		{segOrphan, paths.WorkingDir(dir), "abc"},
	} {
		if err := os.MkdirAll(spec.dir, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(spec.dir, spec.id.String()), []byte(spec.body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	onDisk, err := listLocalSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	fsmBytes := map[glid.GLID]uint64{segFSM: 8192}

	var total int64
	for id, path := range onDisk {
		if sz, ok := fsmBytes[id]; ok {
			total += int64(sz)
			continue
		}
		total += diskusage.FileBytes(path)
	}
	if total != 8192+3 {
		t.Fatalf("total = %d, want 8195 (FSM 8192 + orphan stat 3)", total)
	}
}

func TestLocalPipelineSegmentStorageOrphanUsesFileStat(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	segID := glid.New()
	if err := os.MkdirAll(paths.WorkingDir(root), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.WorkingSegment(root, segID), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}

	vaultID := glid.New()
	o := &Orchestrator{segmentsDir: filepath.Dir(root)}
	o.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true})
	vaultRoot := filepath.Join(o.segmentsDir, vaultID.String())
	if err := os.Rename(root, vaultRoot); err != nil {
		t.Fatal(err)
	}

	got := o.localPipelineSegmentStorageBytes(vaultID)
	if got != 3 {
		t.Fatalf("localPipelineSegmentStorageBytes() = %d, want 3 (orphan working segment)", got)
	}
}

func TestLocalManagedFileStorageBytesFromConfig(t *testing.T) {
	t.Parallel()

	homeRoot := t.TempDir()
	fileID := glid.New()
	path := home.New(homeRoot).ManagedFilePath(fileID.String())
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("xy"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := memory.NewStore()
	if err := store.PutManagedFile(t.Context(), system.ManagedFileConfig{
		ID:   fileID,
		Name: "lookup.mmdb",
		Size: 999,
	}); err != nil {
		t.Fatal(err)
	}

	o := &Orchestrator{homeDir: homeRoot}
	o.setSystemLoader(store)

	if got := o.localManagedFileStorageBytes(t.Context()); got != 999 {
		t.Fatalf("localManagedFileStorageBytes() = %d, want 999 (config size, not 2-byte file)", got)
	}
}

func TestLocalRaftStorageBytes(t *testing.T) {
	t.Parallel()

	homeRoot := t.TempDir()
	raftDir := home.New(homeRoot).RaftDir()
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(raftDir, "wal.log"), []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}

	o := &Orchestrator{homeDir: homeRoot}
	if got := o.localRaftStorageBytes(); got != 5 {
		t.Fatalf("localRaftStorageBytes() = %d, want 5", got)
	}
}

// TestLocalVaultChunkBytesMatchesDiskClaimFormula pins that extracting
// localVaultChunkBytes onto the shared chunk.DiskClaim helper did not
// change its arithmetic: DiskBytes wins when recorded, a cloud-backed
// chunk with no local copy costs nothing, and the fallback (no DiskBytes)
// adds index sizes on top of logical Bytes — exactly the pre-extraction
// formula, now shared with the size-drain trigger.
func TestLocalVaultChunkBytesMatchesDiskClaimFormula(t *testing.T) {
	t.Parallel()

	diskRecorded := chunk.NewChunkID()   // DiskBytes wins over Bytes
	cloudEvicted := chunk.NewChunkID()   // cloud-backed, no local copy: 0
	cloudCached := chunk.NewChunkID()    // cloud-backed, cached: DiskBytes
	fallbackLegacy := chunk.NewChunkID() // no DiskBytes: Bytes + indexes

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	metas := []chunk.ChunkMeta{
		{ID: diskRecorded, WriteStart: base, WriteEnd: base, Bytes: 9000, DiskBytes: 1000, Sealed: true},
		{ID: cloudEvicted, WriteStart: base, WriteEnd: base, Bytes: 9_000_000, DiskBytes: 0, CloudBacked: true, Sealed: true},
		{ID: cloudCached, WriteStart: base, WriteEnd: base, Bytes: 9000, DiskBytes: 700, CloudBacked: true, Sealed: true},
		{ID: fallbackLegacy, WriteStart: base, WriteEnd: base, Bytes: 300, DiskBytes: 0, Sealed: true},
	}
	cm := &retentionFakeChunkManager{chunks: metas}
	im := &retentionFakeIndexManager{sizes: map[chunk.ChunkID]map[string]int64{
		fallbackLegacy: {"token": 50, "attr": 25},
	}}

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	v := NewVaultFromComponents(vaultID, cm, im, nil)
	orch.RegisterVault(v)

	got := orch.localVaultChunkBytes(vaultID)
	// diskRecorded: 1000 (DiskBytes). cloudEvicted: 0 (evicted, no local
	// copy). cloudCached: 700 (DiskBytes, its cache footprint).
	// fallbackLegacy: 300 + 50 + 25 = 375 (Bytes + index sizes).
	want := int64(1000 + 0 + 700 + 375)
	if got != want {
		t.Fatalf("localVaultChunkBytes() = %d, want %d", got, want)
	}
}
