package collection

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// LocalSegmentPresent reports whether segment bytes exist under this home's
// head/, completed/, or pre-head/ layout (probed in that order).
func LocalSegmentPresent(vaultRoot string, segmentID glid.GLID) bool {
	if vaultRoot == "" {
		return false
	}
	_, ok := paths.FindSegment(vaultRoot, segmentID,
		paths.AreaHead, paths.AreaCompleted, paths.AreaPreHead)
	return ok
}
