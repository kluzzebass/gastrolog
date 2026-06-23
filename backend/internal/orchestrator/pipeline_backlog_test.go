package orchestrator

import (
	"path/filepath"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

func TestLocalPipelineDiskSegmentCounts(t *testing.T) {
	dir := t.TempDir()
	vaultID := glid.New()
	segWorking := glid.New()
	segHead := glid.New()

	root := filepath.Join(dir, vaultID.String())
	if err := touchSegmentFile(paths.WorkingDir(root), segWorking); err != nil {
		t.Fatal(err)
	}
	if err := touchSegmentFile(paths.HeadDir(root), segHead); err != nil {
		t.Fatal(err)
	}

	orch := &Orchestrator{segmentsDir: dir}
	got, err := orch.LocalPipelineDiskSegmentCounts(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Working != 1 || got.Head != 1 {
		t.Fatalf("counts = %+v", got)
	}
	if got.CompletedStaging != 0 || got.PreHead != 0 {
		t.Fatalf("unexpected non-zero staging/pre-head: %+v", got)
	}
}

func TestPipelineDiskSegmentCountsAdd(t *testing.T) {
	a := PipelineDiskSegmentCounts{Working: 1, Head: 2}
	b := PipelineDiskSegmentCounts{Working: 3, CompletedStaging: 4}
	a.AddDiskCounts(b)
	if a.Working != 4 || a.Head != 2 || a.CompletedStaging != 4 {
		t.Fatalf("merged = %+v", a)
	}
}
