package paths_test

import (
	"os"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

func TestPurgeSegmentStagingRemovesStagingFiles(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	for _, dir := range []string{paths.HeadDir(root), paths.PreHeadDir(root), paths.CompletedDir(root)} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		paths.HeadSegment(root, segID),
		paths.PreHeadSegment(root, segID),
		paths.CompletedSegment(root, segID),
	} {
		if err := os.WriteFile(path, []byte("seg"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := paths.PurgeSegmentStaging(root, segID); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		paths.HeadSegment(root, segID),
		paths.PreHeadSegment(root, segID),
		paths.CompletedSegment(root, segID),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s removed, stat err=%v", path, err)
		}
	}
}

func TestPurgeHeadStagingLeavesCompleted(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	for _, dir := range []string{paths.HeadDir(root), paths.PreHeadDir(root), paths.CompletedDir(root)} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		paths.HeadSegment(root, segID),
		paths.PreHeadSegment(root, segID),
		paths.CompletedSegment(root, segID),
	} {
		if err := os.WriteFile(path, []byte("seg"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := paths.PurgeHeadStaging(root, segID); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		paths.HeadSegment(root, segID),
		paths.PreHeadSegment(root, segID),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s removed, stat err=%v", path, err)
		}
	}
	if _, err := os.Stat(paths.CompletedSegment(root, segID)); err != nil {
		t.Fatalf("completed segment should remain: %v", err)
	}
}

func TestPurgeSegmentStagingMissingIsNoOp(t *testing.T) {
	t.Parallel()
	if err := paths.PurgeSegmentStaging(t.TempDir(), glid.New()); err != nil {
		t.Fatal(err)
	}
}
