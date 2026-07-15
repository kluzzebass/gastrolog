package chunking

import (
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// SliceRecordBounds scans one inclusive EventID-order slice and returns the
// min/max WriteTS, IngestTS, and SourceTS across its records. The scan reads
// views, not records: bounds need three timestamps per record, and the
// planner runs this constantly — full materialization here was the loaded
// home's single largest allocation source (gastrolog-11y2iv).
func SliceRecordBounds(idx *OrderedIndex, first, last uint32) (vaultctlfsm.ManifestTimeBounds, error) {
	var out vaultctlfsm.ManifestTimeBounds
	if idx == nil {
		return out, nil
	}
	for pos := first; pos <= last; pos++ {
		v, err := idx.ViewAt(pos)
		if err != nil {
			return vaultctlfsm.ManifestTimeBounds{}, err
		}
		mergeRecordBounds(&out, v)
	}
	return out, nil
}

func mergeRecordBounds(b *vaultctlfsm.ManifestTimeBounds, rec record.View) {
	if b.WriteStart.IsZero() || rec.WriteTS.Before(b.WriteStart) {
		b.WriteStart = rec.WriteTS
	}
	if rec.WriteTS.After(b.WriteEnd) {
		b.WriteEnd = rec.WriteTS
	}
	ingest := rec.IngestTS
	if b.IngestStart.IsZero() || ingest.Before(b.IngestStart) {
		b.IngestStart = ingest
	}
	if ingest.After(b.IngestEnd) {
		b.IngestEnd = ingest
	}
	if !rec.SourceTS.IsZero() {
		if b.SourceStart.IsZero() || rec.SourceTS.Before(b.SourceStart) {
			b.SourceStart = rec.SourceTS
		}
		if rec.SourceTS.After(b.SourceEnd) {
			b.SourceEnd = rec.SourceTS
		}
	}
}

// BoundsFromManifestRefs reads local head segments and returns aggregate bounds
// for a sealed/open manifest. Used to enrich inspector metadata when older FSM
// entries lack timestamp fields.
func BoundsFromManifestRefs(refs []vaultctlfsm.OpenChunkSegmentRef, locate SegmentLocator) (vaultctlfsm.ManifestTimeBounds, error) {
	var out vaultctlfsm.ManifestTimeBounds
	for _, ref := range refs {
		path, ok := locate.SegmentPath(ref.SegmentID)
		if !ok {
			continue
		}
		idx, err := BuildOrderedIndex(path)
		if err != nil {
			return vaultctlfsm.ManifestTimeBounds{}, err
		}
		slice, err := SliceRecordBounds(idx, ref.FirstRecordNumber, ref.LastRecordNumber)
		_ = idx.Close()
		if err != nil {
			return vaultctlfsm.ManifestTimeBounds{}, err
		}
		mergeManifestBounds(&out, slice)
	}
	return out, nil
}

func mergeManifestBounds(dst *vaultctlfsm.ManifestTimeBounds, src vaultctlfsm.ManifestTimeBounds) {
	if src.WriteStart.IsZero() && src.WriteEnd.IsZero() &&
		src.IngestStart.IsZero() && src.IngestEnd.IsZero() &&
		src.SourceStart.IsZero() && src.SourceEnd.IsZero() {
		return
	}
	if !src.WriteStart.IsZero() {
		if dst.WriteStart.IsZero() || src.WriteStart.Before(dst.WriteStart) {
			dst.WriteStart = src.WriteStart
		}
	}
	if src.WriteEnd.After(dst.WriteEnd) {
		dst.WriteEnd = src.WriteEnd
	}
	if !src.IngestStart.IsZero() {
		if dst.IngestStart.IsZero() || src.IngestStart.Before(dst.IngestStart) {
			dst.IngestStart = src.IngestStart
		}
	}
	if src.IngestEnd.After(dst.IngestEnd) {
		dst.IngestEnd = src.IngestEnd
	}
	if !src.SourceStart.IsZero() {
		if dst.SourceStart.IsZero() || src.SourceStart.Before(dst.SourceStart) {
			dst.SourceStart = src.SourceStart
		}
	}
	if src.SourceEnd.After(dst.SourceEnd) {
		dst.SourceEnd = src.SourceEnd
	}
}
