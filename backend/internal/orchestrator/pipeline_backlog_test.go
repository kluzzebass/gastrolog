package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

func TestLocalPipelineDiskSegmentCounts(t *testing.T) {
	dir := t.TempDir()
	vaultID := glid.New()
	segWorking := glid.New()
	segCompleted := glid.New()
	segHead := glid.New()
	segPreHead := glid.New()

	root := filepath.Join(dir, vaultID.String())
	if err := touchSegmentFile(paths.WorkingDir(root), segWorking); err != nil {
		t.Fatal(err)
	}
	if err := touchSegmentFile(paths.CompletedDir(root), segCompleted); err != nil {
		t.Fatal(err)
	}
	if err := touchSegmentFile(paths.PreHeadDir(root), segPreHead); err != nil {
		t.Fatal(err)
	}
	if err := touchSegmentFile(paths.HeadDir(root), segHead); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.HeadDir(root), segHead.String()), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}

	orch := &Orchestrator{segmentsDir: dir}
	got, err := orch.LocalPipelineDiskSegmentCounts(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Working != 1 || got.CompletedStaging != 1 || got.PreHead != 1 || got.Head != 1 {
		t.Fatalf("counts = %+v", got)
	}
	if got.WorkingBytes != 1 || got.CompletedStagingBytes != 1 || got.PreHeadBytes != 1 || got.HeadBytes != 5 {
		t.Fatalf("bytes = %+v", got)
	}
}

func TestPipelineDiskSegmentCountsAdd(t *testing.T) {
	a := PipelineDiskSegmentCounts{Working: 1, Head: 2, WorkingBytes: 10, HeadBytes: 20}
	b := PipelineDiskSegmentCounts{Working: 3, CompletedStaging: 4, WorkingBytes: 30, CompletedStagingBytes: 40}
	a.AddDiskCounts(b)
	if a.Working != 4 || a.Head != 2 || a.CompletedStaging != 4 {
		t.Fatalf("merged counts = %+v", a)
	}
	if a.WorkingBytes != 40 || a.HeadBytes != 20 || a.CompletedStagingBytes != 40 {
		t.Fatalf("merged bytes = %+v", a)
	}
}
