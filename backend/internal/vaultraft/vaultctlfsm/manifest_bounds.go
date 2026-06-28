package vaultctlfsm

import (
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
)

// ManifestTimeBounds holds running min/max record timestamps for a manifest.
type ManifestTimeBounds struct {
	WriteStart  time.Time
	WriteEnd    time.Time
	IngestStart time.Time
	IngestEnd   time.Time
	SourceStart time.Time
	SourceEnd   time.Time
}

func (b ManifestTimeBounds) IsZero() bool {
	return b.WriteStart.IsZero() && b.WriteEnd.IsZero() &&
		b.IngestStart.IsZero() && b.IngestEnd.IsZero() &&
		b.SourceStart.IsZero() && b.SourceEnd.IsZero()
}

func (b ManifestTimeBounds) isZero() bool {
	return b.IsZero()
}

func mergeManifestTimeBounds(dst *ManifestTimeBounds, writeStart, writeEnd, ingestStart, ingestEnd, sourceStart, sourceEnd time.Time) {
	mergeTimeMin(&dst.WriteStart, writeStart)
	mergeTimeMax(&dst.WriteEnd, writeEnd)
	mergeTimeMin(&dst.IngestStart, ingestStart)
	mergeTimeMax(&dst.IngestEnd, ingestEnd)
	if !sourceStart.IsZero() {
		if dst.SourceStart.IsZero() || sourceStart.Before(dst.SourceStart) {
			dst.SourceStart = sourceStart
		}
	}
	if !sourceEnd.IsZero() {
		if sourceEnd.After(dst.SourceEnd) {
			dst.SourceEnd = sourceEnd
		}
	}
}

func mergeTimeMin(dst *time.Time, v time.Time) {
	if v.IsZero() {
		return
	}
	if dst.IsZero() || v.Before(*dst) {
		*dst = v
	}
}

func mergeTimeMax(dst *time.Time, v time.Time) {
	if v.IsZero() {
		return
	}
	if v.After(*dst) {
		*dst = v
	}
}

func boundsFromAddRefCommand(c *gastrologv1.AddOpenChunkSegmentRefCommand) ManifestTimeBounds {
	if c == nil {
		return ManifestTimeBounds{}
	}
	return manifestBoundsFromProto(
		c.GetWriteStartNanos(),
		c.GetWriteEndNanos(),
		c.GetIngestStartNanos(),
		c.GetIngestEndNanos(),
		c.GetSourceStartNanos(),
		c.GetSourceEndNanos(),
	)
}

func boundsFromRefEntry(c *gastrologv1.AddOpenChunkSegmentRefEntry) ManifestTimeBounds {
	if c == nil {
		return ManifestTimeBounds{}
	}
	return manifestBoundsFromProto(
		c.GetWriteStartNanos(),
		c.GetWriteEndNanos(),
		c.GetIngestStartNanos(),
		c.GetIngestEndNanos(),
		c.GetSourceStartNanos(),
		c.GetSourceEndNanos(),
	)
}

// MergeManifestTimeBounds folds src into dst (min/max per axis).
func MergeManifestTimeBounds(dst *ManifestTimeBounds, src ManifestTimeBounds) {
	mergeManifestTimeBounds(dst,
		src.WriteStart, src.WriteEnd,
		src.IngestStart, src.IngestEnd,
		src.SourceStart, src.SourceEnd,
	)
}

func manifestBoundsToProto(b ManifestTimeBounds) (writeStart, writeEnd, ingestStart, ingestEnd, sourceStart, sourceEnd int64) {
	if !b.WriteStart.IsZero() {
		writeStart = b.WriteStart.UnixNano()
	}
	if !b.WriteEnd.IsZero() {
		writeEnd = b.WriteEnd.UnixNano()
	}
	if !b.IngestStart.IsZero() {
		ingestStart = b.IngestStart.UnixNano()
	}
	if !b.IngestEnd.IsZero() {
		ingestEnd = b.IngestEnd.UnixNano()
	}
	if !b.SourceStart.IsZero() {
		sourceStart = b.SourceStart.UnixNano()
	}
	if !b.SourceEnd.IsZero() {
		sourceEnd = b.SourceEnd.UnixNano()
	}
	return writeStart, writeEnd, ingestStart, ingestEnd, sourceStart, sourceEnd
}

func manifestBoundsFromProto(writeStart, writeEnd, ingestStart, ingestEnd, sourceStart, sourceEnd int64) ManifestTimeBounds {
	return ManifestTimeBounds{
		WriteStart:  saneManifestTime(time.Unix(0, writeStart)),
		WriteEnd:    saneManifestTime(time.Unix(0, writeEnd)),
		IngestStart: saneManifestTime(time.Unix(0, ingestStart)),
		IngestEnd:   saneManifestTime(time.Unix(0, ingestEnd)),
		SourceStart: saneManifestTime(time.Unix(0, sourceStart)),
		SourceEnd:   saneManifestTime(time.Unix(0, sourceEnd)),
	}
}

// saneManifestTime drops Unix-epoch sentinel timestamps that leak into the UI
// when older FSM rows lack bounds fields.
func saneManifestTime(t time.Time) time.Time {
	if t.IsZero() || t.Year() < 2000 {
		return time.Time{}
	}
	return t
}

func applyManifestBoundsToEntry(e *ManifestEntry, b ManifestTimeBounds) {
	if e == nil || b.isZero() {
		return
	}
	if !b.WriteStart.IsZero() {
		e.WriteStart = b.WriteStart
	}
	if !b.WriteEnd.IsZero() {
		e.WriteEnd = b.WriteEnd
	}
	if !b.IngestStart.IsZero() {
		e.IngestStart = b.IngestStart
	}
	if !b.IngestEnd.IsZero() {
		e.IngestEnd = b.IngestEnd
	}
	if !b.SourceStart.IsZero() {
		e.SourceStart = b.SourceStart
	}
	if !b.SourceEnd.IsZero() {
		e.SourceEnd = b.SourceEnd
	}
}

func manifestEntryFromOpenChunk(m *OpenChunkManifest, state chunk.ChunkState) ManifestEntry {
	entry := ManifestEntry{
		ID:          m.ChunkID,
		WriteStart:  m.OpenedAt,
		IngestStart: m.OpenedAt,
		SourceStart: m.OpenedAt,
		SealedAt:    m.SealedAt,
		State:       state,
		RecordCount: int64(m.TotalRecords), //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
		Bytes:       int64(m.TotalBytes),   //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
	}
	applyManifestBoundsToEntry(&entry, m.Bounds)
	return entry
}

// ApplyManifestBoundsToChunkMeta copies non-zero manifest bounds onto ChunkMeta.
func ApplyManifestBoundsToChunkMeta(m *chunk.ChunkMeta, b ManifestTimeBounds) {
	if m == nil {
		return
	}
	if !b.WriteStart.IsZero() {
		m.WriteStart = b.WriteStart
	}
	if !b.WriteEnd.IsZero() {
		m.WriteEnd = b.WriteEnd
	}
	if !b.IngestStart.IsZero() {
		m.IngestStart = b.IngestStart
	}
	if !b.IngestEnd.IsZero() {
		m.IngestEnd = b.IngestEnd
	}
	if !b.SourceStart.IsZero() {
		m.SourceStart = b.SourceStart
	}
	if !b.SourceEnd.IsZero() {
		m.SourceEnd = b.SourceEnd
	}
}
