package collection

import (
	"os"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

func TestLocalSegmentPresent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	if LocalSegmentPresent(root, segID) {
		t.Fatal("expected absent before write")
	}
	if err := paths.EnsureHeadDir(root); err != nil {
		t.Fatal(err)
	}
	path := paths.HeadSegment(root, segID)
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !LocalSegmentPresent(root, segID) {
		t.Fatal("expected present under head/")
	}
}
