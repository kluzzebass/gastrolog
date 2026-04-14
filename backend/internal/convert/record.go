// Package convert provides canonical converters between internal domain types
// and their protobuf representations. Every proto ↔ domain conversion for a
// given type pair MUST go through the single function defined here so that
// field coverage can never silently diverge between code paths.
package convert

import (
	"gastrolog/internal/glid"
	"maps"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RecordToExport converts a chunk.Record to a proto ExportRecord with all
// fields populated. This is the single canonical Record → ExportRecord
// converter. All code paths that serialize a chunk.Record for wire
// transmission (tier transfers, ingestion forwarding, vault export,
// cross-node search) MUST use this function.
//
// Zero-valued fields serialize naturally: a zero UUID becomes
// "00000000-0000-0000-0000-000000000000" (parsed back as glid.Nil by
// ExportToRecord), and zero timestamps become nil proto fields.
func RecordToExport(rec chunk.Record) *gastrologv1.ExportRecord {
	er := &gastrologv1.ExportRecord{
		Raw:        rec.Raw,
		VaultId:    rec.VaultID.ToProto(),
		ChunkId:    glid.GLID(rec.Ref.ChunkID).ToProto(),
		Pos:        rec.Ref.Pos,
		IngestSeq:  rec.EventID.IngestSeq,
		IngesterId: rec.EventID.IngesterID[:],
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
		er.Attrs = make(map[string]string, len(rec.Attrs))
		maps.Copy(er.Attrs, rec.Attrs)
	}
	return er
}

// ExportToRecord converts a proto ExportRecord to a chunk.Record with all
// fields populated. This is the single canonical ExportRecord → Record
// converter. All code paths that deserialize a wire ExportRecord (import
// handlers, search result collection, tier replication) MUST use this
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
	rec.EventID.IngestTS = rec.IngestTS
	return rec
}
