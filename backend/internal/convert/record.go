// Package convert provides canonical converters between internal domain types
// and their protobuf representations. Every proto ↔ domain conversion for a
// given type pair MUST go through the single function defined here so that
// field coverage can never silently diverge between code paths.
package convert

import (
	"maps"
	"slices"
	"strings"

	"gastrolog/internal/glid"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/record"
	"gastrolog/internal/safeutf8"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RecordToExport converts a chunk.Record to a proto ExportRecord with all
// fields populated. This is the single canonical Record → ExportRecord
// converter. All code paths that serialize a chunk.Record for wire
// transmission (vault transfers, ingestion forwarding, vault export,
// cross-node search) MUST use this function.
//
// Zero-valued fields serialize naturally: a zero UUID becomes
// "00000000-0000-0000-0000-000000000000" (parsed back as glid.Nil by
// ExportToRecord), and zero timestamps become nil proto fields.
func RecordToExport(rec chunk.Record) *gastrologv1.ExportRecord {
	er := &gastrologv1.ExportRecord{
		Raw:        slices.Clone(rec.Raw),
		VaultId:    rec.VaultID.ToProto(),
		ChunkId:    glid.GLID(rec.Ref.ChunkID).ToProto(),
		Pos:        rec.Ref.Pos,
		IngestSeq:  rec.EventID.IngestSeq,
		IngesterId: rec.EventID.IngesterID[:],
		NodeId:     rec.EventID.NodeID[:],
	}
	if !rec.SourceTS.IsZero() {
		er.SourceTs = timestamppb.New(rec.SourceTS)
	}
	if !rec.IngestTS.IsZero() {
		er.IngestTs = timestamppb.New(rec.IngestTS)
	}
	if !rec.WriteTS.IsZero() {
		er.WriteTs = timestamppb.New(rec.WriteTS)
	}
	if len(rec.Attrs) > 0 {
		// Attrs are proto3 map<string,string>; invalid UTF-8 would fail
		// marshal. Ingesters normally produce clean attrs, but raw
		// message bytes sometimes leak in — sanitize at the wire
		// boundary. Clone every key/value so the export is detached
		// from mmap-backed dict strings in the source record.
		sanitized := safeutf8.Attrs(rec.Attrs)
		er.Attrs = make(map[string]string, len(sanitized))
		for k, v := range sanitized {
			er.Attrs[strings.Clone(k)] = strings.Clone(v)
		}
	}
	return er
}

// ExportToRecord converts a proto ExportRecord to a chunk.Record with all
// fields populated. This is the single canonical ExportRecord → Record
// converter. All code paths that deserialize a wire ExportRecord (import
// handlers, search result collection, vault replication) MUST use this
// function.
func ExportToRecord(er *gastrologv1.ExportRecord) chunk.Record {
	rec := chunk.Record{Raw: er.GetRaw()}
	if er.GetSourceTs() != nil {
		rec.SourceTS = er.GetSourceTs().AsTime()
	}
	if er.GetIngestTs() != nil {
		rec.IngestTS = er.GetIngestTs().AsTime()
	}
	if er.GetWriteTs() != nil {
		rec.WriteTS = er.GetWriteTs().AsTime()
	}
	if len(er.GetAttrs()) > 0 {
		rec.Attrs = make(chunk.Attributes, len(er.GetAttrs()))
		maps.Copy(rec.Attrs, er.GetAttrs())
	}
	if len(er.GetVaultId()) >= glid.Size {
		rec.VaultID = glid.FromBytes(er.GetVaultId())
	}
	if len(er.GetChunkId()) >= glid.Size {
		rec.Ref.ChunkID = chunk.ChunkID(glid.FromBytes(er.GetChunkId()))
		rec.Ref.Pos = er.GetPos()
	}
	rec.EventID.IngestSeq = er.GetIngestSeq()
	if len(er.GetIngesterId()) == 16 {
		copy(rec.EventID.IngesterID[:], er.GetIngesterId())
	}
	if len(er.GetNodeId()) == 16 {
		copy(rec.EventID.NodeID[:], er.GetNodeId())
	}
	rec.EventID.IngestTS = rec.IngestTS
	return rec
}

// ChunkToRecord converts a chunk.Record (the legacy storage/query type) to a
// record.Record (the pipeline write type), dropping the query-only Ref/VaultID
// fields. This is the single canonical chunk.Record → record.Record converter,
// used by non-ingester writers (retention fan-out, ImportRecords,
// export-to-vault) that inject records into the pipeline submit API while
// preserving the original EventID.
func ChunkToRecord(rec chunk.Record) record.Record {
	out := record.Record{
		SourceTS: rec.SourceTS,
		IngestTS: rec.IngestTS,
		WriteTS:  rec.WriteTS,
		EventID: record.EventID{
			IngesterID: rec.EventID.IngesterID,
			NodeID:     rec.EventID.NodeID,
			IngestTS:   rec.EventID.IngestTS,
			IngestSeq:  rec.EventID.IngestSeq,
		},
		Raw:            rec.Raw,
		WaitForReplica: rec.WaitForReplica,
	}
	if len(rec.Attrs) > 0 {
		out.Attrs = make(record.Attributes, len(rec.Attrs))
		maps.Copy(out.Attrs, rec.Attrs)
	}
	return out
}

// ChunkToRecordOwned is ChunkToRecord for callers that exclusively own
// rec.Attrs (freshly materialized per record, e.g. a retention drain
// cursor): the map transfers without a defensive copy. Raw already
// transfers by reference in both variants; this extends the same
// ownership contract to Attrs. Per-record attrs copies on the drain
// path were a measurable slice of GC churn (gastrolog-11y2iv).
func ChunkToRecordOwned(rec chunk.Record) record.Record {
	out := ChunkToRecord(chunk.Record{
		SourceTS:       rec.SourceTS,
		IngestTS:       rec.IngestTS,
		WriteTS:        rec.WriteTS,
		EventID:        rec.EventID,
		Raw:            rec.Raw,
		WaitForReplica: rec.WaitForReplica,
	})
	out.Attrs = record.Attributes(rec.Attrs)
	return out
}
