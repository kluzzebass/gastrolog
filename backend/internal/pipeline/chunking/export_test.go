package chunking

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// Test-only exports (gastrolog-2v9d67). Production code has no callers for
// these seams; they live here so the external chunking_test package can
// reach package internals without keeping dead symbols in shipping code.

// HeadSegmentLocator resolves segments present under vaultRoot/head/ only —
// unlike the production VaultSegmentLocator (locator.go), which also probes
// completed/. Purge tests rely on the head-only probe to assert a segment is
// gone from head/ regardless of other areas.
type HeadSegmentLocator struct {
	Root string
}

func (l HeadSegmentLocator) SegmentPath(segmentID glid.GLID) (string, bool) {
	return paths.FindSegment(l.Root, segmentID, paths.AreaHead)
}

// CatchUpBudget exposes catchUpBudget for budget-scaling tests.
func CatchUpBudget(eligible int, policy ManifestRotationPolicy) int {
	return catchUpBudget(eligible, policy)
}

// IsGLCBBuildTmpName exposes isGLCBBuildTmpName so the external
// chunking_test package can pin the BuildGLCBFile-writer /
// sweepOrphanGLCBBuildTmp-sweeper naming contract (gastrolog-66hmx3)
// without needing an internal-package test file.
func IsGLCBBuildTmpName(name string) bool {
	return isGLCBBuildTmpName(name)
}

// GLCBBuildTmpPrefix exposes glcbBuildTmpPrefix, the exact os.CreateTemp
// pattern prefix BuildGLCBFile uses, so contract tests can drive the real
// writer call instead of retyping the literal.
const GLCBBuildTmpPrefix = glcbBuildTmpPrefix
