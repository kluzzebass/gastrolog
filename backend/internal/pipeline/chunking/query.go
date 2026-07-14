package chunking

import (
	"errors"
	"iter"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
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
		span, err := RefToSpan(ManifestRef{
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

// openChunkRecordPos locates one merged-order record: its mapping and the
// frame offset inside it.
type openChunkRecordPos struct {
	seg     *segment.MappedSegment
	filePos uint32
}

// OpenChunkReader serves record-at-position reads over the SAME merged,
// EventID-deduplicated order that QueryOpenChunk streams: both derive from
// mergeSpanEntries over identically resolved span refs, so a position handed
// out by the forward stream always resolves to the same record here — there
// is no second record-at-position arithmetic to drift (gastrolog-54mjat).
//
// Segments are mapped once at construction and stay mapped until Close —
// intended for one search-cursor lifetime. The previous per-call path
// re-opened and full-CRC-verified a segment (segment.Open) for EVERY
// positional read, making a reverse scan O(records × segment bytes).
//
// Lifetime safety: manifest-listed segments are finalized and immutable —
// they are never rewritten in place. Head purge unlinks them, which leaves
// existing mappings readable until Close (same guarantee MergeSpanViews
// already relies on for the forward stream). A purge racing construction
// surfaces as an open error, never as torn reads.
type OpenChunkReader struct {
	positions []openChunkRecordPos
	closeAll  func()
}

// NewOpenChunkReader resolves the manifest like QueryOpenChunk (missing local
// segments are omitted and reported, sealed refs are deduplicated by EventID)
// and builds the position table from one index-entry merge — no frame reads.
func NewOpenChunkReader(in OpenChunkQueryInput) (*OpenChunkReader, OpenChunkQueryReport, error) {
	openRefs, missing, err := OpenChunkSpanRefs(in.Manifest, in.Locate)
	if err != nil {
		return nil, OpenChunkQueryReport{}, err
	}
	refs := append(append([]SpanRef(nil), in.SealedRefs...), openRefs...)
	report := OpenChunkQueryReport{MissingSegments: missing}
	if len(refs) == 0 {
		return &OpenChunkReader{}, report, nil
	}
	cursors, closeAll, err := loadSpanCursors(refs)
	if err != nil {
		return nil, report, err
	}
	var capacity uint64
	for _, ref := range refs {
		capacity += uint64(ref.Span.Count)
	}
	positions := make([]openChunkRecordPos, 0, capacity)
	var prev record.EventID
	havePrev := false
	for step, err := range mergeSpanEntries(cursors) {
		if err != nil {
			closeAll()
			return nil, report, err
		}
		if havePrev && step.entry.EventID.Compare(prev) == 0 {
			continue
		}
		havePrev = true
		prev = step.entry.EventID
		positions = append(positions, openChunkRecordPos{
			seg:     cursors[step.cur].seg,
			filePos: step.entry.FilePos,
		})
	}
	return &OpenChunkReader{positions: positions, closeAll: closeAll}, report, nil
}

// Len returns the number of records in the merged, deduplicated order —
// the stream length QueryOpenChunk would yield for the same input.
func (r *OpenChunkReader) Len() uint64 {
	return uint64(len(r.positions))
}

// ReadAt returns the record at global 1-based position pos within the
// merged, deduplicated order.
func (r *OpenChunkReader) ReadAt(pos uint64) (record.Record, error) {
	if pos == 0 || pos > uint64(len(r.positions)) {
		return record.Record{}, ErrManifestRecordOutOfRange
	}
	p := r.positions[pos-1]
	return p.seg.RecordAtFilePos(p.filePos)
}

// Close unmaps the reader's segments. Records returned by ReadAt are
// self-contained copies and remain valid after Close.
func (r *OpenChunkReader) Close() error {
	if r.closeAll != nil {
		r.closeAll()
		r.closeAll = nil
	}
	return nil
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
