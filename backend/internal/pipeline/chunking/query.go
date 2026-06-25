package chunking

import (
	"errors"
	"iter"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrNoOpenChunkManifest is returned when QueryOpenChunk is called without a manifest.
var ErrNoOpenChunkManifest = errors.New("no open chunk manifest")

// ErrManifestRecordOutOfRange is returned when a 1-based manifest record position
// is zero or greater than TotalRecords.
var ErrManifestRecordOutOfRange = errors.New("manifest record position out of range")

// OpenChunkQueryInput configures virtual open-chunk reads (direction D).
// The open chunk has no GLCB file; records come from manifest-listed segment
// spans resolved against local head/completed paths.
type OpenChunkQueryInput struct {
	Manifest *vaultctlfsm.OpenChunkManifest
	Locate   SegmentLocator
	// SealedRefs optionally includes spans from already-sealed GLCB chunks.
	// The merged stream deduplicates by EventID (handoff overlap); either copy wins.
	SealedRefs []SpanRef
}

// OpenChunkQueryReport summarizes local coverage for one manifest read.
type OpenChunkQueryReport struct {
	// MissingSegments lists manifest refs with no local segment file.
	// Lagging replicas may still serve a partial view from present refs.
	MissingSegments []glid.GLID
}

// OpenChunkSpanRefs resolves manifest refs to local span refs. Missing segments
// are returned separately; present refs are still queryable on partial homes.
func OpenChunkSpanRefs(m *vaultctlfsm.OpenChunkManifest, locate SegmentLocator) ([]SpanRef, []glid.GLID, error) {
	if m == nil {
		return nil, nil, ErrNoOpenChunkManifest
	}
	if locate == nil {
		return nil, nil, errors.New("segment locator required")
	}
	refs := make([]SpanRef, 0, len(m.Refs))
	var missing []glid.GLID
	for _, ref := range m.Refs {
		path, ok := locate.SegmentPath(ref.SegmentID)
		if !ok {
			missing = append(missing, ref.SegmentID)
			continue
		}
		span, err := RefToSpan(ManifestRefEntry{
			SegmentID:         ref.SegmentID,
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
		})
		if err != nil {
			return nil, missing, err
		}
		refs = append(refs, SpanRef{Path: path, Span: span})
	}
	return refs, missing, nil
}

// QueryOpenChunk streams records from the open manifest in canonical EventID order.
// Missing local segments are omitted; see OpenChunkQueryReport.MissingSegments.
func QueryOpenChunk(in OpenChunkQueryInput) (iter.Seq2[record.Record, error], OpenChunkQueryReport, error) {
	openRefs, missing, err := OpenChunkSpanRefs(in.Manifest, in.Locate)
	if err != nil {
		return nil, OpenChunkQueryReport{}, err
	}
	refs := append(append([]SpanRef(nil), in.SealedRefs...), openRefs...)
	report := OpenChunkQueryReport{MissingSegments: missing}
	return mergeSpanRefsDedup(refs), report, nil
}

// CollectOpenChunk materializes QueryOpenChunk (primarily for tests).
func CollectOpenChunk(in OpenChunkQueryInput) ([]record.Record, OpenChunkQueryReport, error) {
	seq, report, err := QueryOpenChunk(in)
	if err != nil {
		return nil, report, err
	}
	var out []record.Record
	for rec, err := range seq {
		if err != nil {
			return nil, report, err
		}
		out = append(out, rec)
	}
	return out, report, nil
}

// ReadManifestRecordAt returns the record at global 1-based position pos within
// the manifest's canonical EventID merge order.
func ReadManifestRecordAt(m *vaultctlfsm.OpenChunkManifest, locate SegmentLocator, pos uint64) (record.Record, error) {
	if m == nil || pos == 0 || pos > m.TotalRecords {
		return record.Record{}, ErrManifestRecordOutOfRange
	}
	remaining := pos
	for _, ref := range m.Refs {
		refCount := uint64(ref.LastRecordNumber-ref.FirstRecordNumber) + 1
		if remaining > refCount {
			remaining -= refCount
			continue
		}
		path, ok := locate.SegmentPath(ref.SegmentID)
		if !ok {
			return record.Record{}, &MissingSegmentsError{SegmentIDs: []glid.GLID{ref.SegmentID}}
		}
		idx, err := BuildOrderedIndex(path)
		if err != nil {
			return record.Record{}, err
		}
		segPos := ref.FirstRecordNumber + uint32(remaining-1) //nolint:gosec // G115: remaining <= refCount
		rec, err := idx.RecordAt(segPos)
		_ = idx.Close()
		if err != nil {
			return record.Record{}, err
		}
		return rec, nil
	}
	return record.Record{}, ErrManifestRecordOutOfRange
}

// RecordToChunk converts a pipeline segment record into chunk.Record for query.
func RecordToChunk(rec record.Record) chunk.Record {
	return chunk.Record{
		SourceTS: rec.SourceTS,
		IngestTS: rec.IngestTS,
		WriteTS:  rec.WriteTS,
		EventID: chunk.EventID{
			IngesterID: rec.EventID.IngesterID,
			NodeID:     rec.EventID.NodeID,
			IngestTS:   rec.EventID.IngestTS,
			IngestSeq:  rec.EventID.IngestSeq,
		},
		Attrs: chunk.Attributes(rec.Attrs),
		Raw:   rec.Raw,
	}
}

func mergeSpanRefsDedup(refs []SpanRef) iter.Seq2[record.Record, error] {
	return func(yield func(record.Record, error) bool) {
		var prev record.EventID
		var havePrev bool
		for rec, err := range MergeSpanRefs(refs) {
			if err != nil {
				yield(record.Record{}, err)
				return
			}
			if havePrev && rec.EventID.Compare(prev) == 0 {
				continue
			}
			havePrev = true
			prev = rec.EventID
			if !yield(rec, nil) {
				return
			}
		}
	}
}
