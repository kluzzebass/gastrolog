package chunking

import (
	"os"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// VaultSegmentLocator resolves indexed segments under vaultRoot head/ or completed/.
type VaultSegmentLocator struct {
	Root string
}

func (l VaultSegmentLocator) SegmentPath(segmentID glid.GLID) (string, bool) {
	for _, path := range []string{
		paths.HeadSegment(l.Root, segmentID),
		paths.CompletedSegment(l.Root, segmentID),
	} {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}
