package distribution

import (
	"context"
	"os"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segmentation"
)

// Metadata is the vault-ctl publish payload for a completed segment.
type Metadata struct {
	SegmentID     glid.GLID
	VaultID       glid.GLID
	RecordCount   uint32
	ByteSize      uint64
	FirstIngestTS time.Time
	LastIngestTS  time.Time
	Checksum      uint32
}

// Publisher publishes completed segment metadata to the vault-ctl log.
type Publisher interface {
	Publish(ctx context.Context, meta Metadata) error
}

// MetadataFrom builds publish metadata from a completed segment on disk.
func MetadataFrom(seg segmentation.CompletedSegment) (Metadata, error) {
	info, err := os.Stat(seg.Path)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{
		SegmentID:     seg.Meta.ID,
		VaultID:       seg.VaultID,
		RecordCount:   seg.Header.RecordCount,
		ByteSize:      uint64(info.Size()), //nolint:gosec // G115: segment file size bounded
		FirstIngestTS: seg.Header.FirstIngestTS,
		LastIngestTS:  seg.Header.LastIngestTS,
		Checksum:      seg.Header.SegmentChecksum,
	}, nil
}
