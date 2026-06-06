package chunking

import (
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// SealedManifest is the frozen open-chunk manifest awaiting local GLCB build.
type SealedManifest struct {
	ChunkID  chunk.ChunkID
	OpenedAt time.Time
	SealedAt time.Time
	Refs     []ManifestRefEntry
}

// ManifestRefEntry names one segment slice in EventID-sorted record numbers.
type ManifestRefEntry struct {
	SegmentID         glid.GLID
	FirstRecordNumber uint32
	LastRecordNumber  uint32
}

// RefToSpan converts inclusive last record number to Start/Count span semantics.
func RefToSpan(ref ManifestRefEntry) (Span, error) {
	if ref.LastRecordNumber < ref.FirstRecordNumber {
		return Span{}, ErrInvalidManifestRef
	}
	count := ref.LastRecordNumber - ref.FirstRecordNumber + 1
	return Span{
		SegmentID: ref.SegmentID,
		Start:     ref.FirstRecordNumber,
		Count:     count,
	}, nil
}
