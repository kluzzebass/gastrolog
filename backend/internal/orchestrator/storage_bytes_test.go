package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

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
	o := &Orchestrator{
		segmentsDir:    filepath.Dir(root),
		pipelineVaults: map[glid.GLID]pipelineVaultReg{vaultID: {home: true}},
	}
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

	o := &Orchestrator{
		homeDir:   homeRoot,
		sysLoader: store,
	}

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
