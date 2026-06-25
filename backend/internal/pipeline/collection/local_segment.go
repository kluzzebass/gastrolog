package collection

import (
	"os"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// LocalSegmentPresent reports whether segment bytes exist under this home's
// head/, completed/, or pre-head/ layout.
func LocalSegmentPresent(vaultRoot string, segmentID glid.GLID) bool {
	if vaultRoot == "" {
		return false
	}
	for _, path := range []string{
		paths.HeadSegment(vaultRoot, segmentID),
		paths.CompletedSegment(vaultRoot, segmentID),
		paths.PreHeadSegment(vaultRoot, segmentID),
	} {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}
	return false
}
