package distribution

import (
	"context"
	"os"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
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
	return metadataFromPath(seg.Path, seg.VaultID, seg.Meta.ID, seg.Header)
}

func metadataFromPath(path string, vaultID, segID glid.GLID, hdr segment.Header) (Metadata, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{
		SegmentID:     segID,
		VaultID:       vaultID,
		RecordCount:   hdr.RecordCount,
		ByteSize:      uint64(info.Size()), //nolint:gosec // G115: segment file size bounded
		FirstIngestTS: hdr.FirstIngestTS,
		LastIngestTS:  hdr.LastIngestTS,
		Checksum:      hdr.SegmentChecksum,
	}, nil
}
