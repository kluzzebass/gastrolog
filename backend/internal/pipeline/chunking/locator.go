package chunking

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// VaultSegmentLocator resolves indexed segments under vaultRoot head/ or
// completed/ (probed in that order).
type VaultSegmentLocator struct {
	Root string
}

func (l VaultSegmentLocator) SegmentPath(segmentID glid.GLID) (string, bool) {
	return paths.FindSegment(l.Root, segmentID, paths.AreaHead, paths.AreaCompleted)
}
