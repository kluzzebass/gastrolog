package collection

import (
	"path/filepath"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// TestIsPreHeadPullingName_MatchesRealPullToPreHeadTmpPath pins the
// writer <-> sweeper contract for pre-head/*.pulling temp files:
// PullToPreHead's tmp path is literally finalPath + preHeadPullSuffix
// (transfer.go). This drives that exact same construction — not a hand-typed
// pattern guess — and asserts isPreHeadPullingName (the predicate
// sweepOrphanPullingFiles uses) matches it, while leaving the final
// (post-rename) name alone.
func TestIsPreHeadPullingName_MatchesRealPullToPreHeadTmpPath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()

	finalPath := paths.PreHeadSegment(root, segID)
	tmpPath := finalPath + preHeadPullSuffix // exact construction from PullToPreHead

	if !isPreHeadPullingName(filepath.Base(tmpPath)) {
		t.Fatalf("isPreHeadPullingName(%q) = false, want true (this is PullToPreHead's real tmp name)", filepath.Base(tmpPath))
	}
	if isPreHeadPullingName(filepath.Base(finalPath)) {
		t.Fatalf("isPreHeadPullingName(%q) = true, want false (this is the promoted-in-place final pre-head name)", filepath.Base(finalPath))
	}
}
