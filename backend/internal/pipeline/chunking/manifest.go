package chunking

import (
	"time"

	"gastrolog/internal/chunk"
)

// SealedManifest is the frozen open-chunk manifest awaiting local GLCB build.
type SealedManifest struct {
	ChunkID  chunk.ChunkID
	OpenedAt time.Time
	SealedAt time.Time
	Refs     []ManifestRef
}

// RefToSpan converts inclusive last record number to Start/Count span semantics.
func RefToSpan(ref ManifestRef) (Span, error) {
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
