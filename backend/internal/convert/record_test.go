package convert

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fullyPopulatedRecord returns a chunk.Record with every field set to a
// non-zero, distinguishable value. Tests that assert round-trip fidelity
// should use this — any field that silently drops to zero will be caught.
func fullyPopulatedRecord() chunk.Record {
	return chunk.Record{
		Raw:      []byte("log line: something happened"),
		SourceTS: time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC),
		IngestTS: time.Date(2025, 6, 15, 10, 30, 1, 0, time.UTC),
		WriteTS:  time.Date(2025, 6, 15, 10, 30, 2, 0, time.UTC),
		EventID: chunk.EventID{
			IngesterID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			IngestSeq:  42,
			IngestTS:   time.Date(2025, 6, 15, 10, 30, 1, 0, time.UTC),
		},
		Attrs: chunk.Attributes{
			"host":  "web-1",
			"level": "error",
		},
		Ref: chunk.RecordRef{
			ChunkID: chunk.ChunkID(uuid.MustParse("22222222-2222-2222-2222-222222222222")),
			Pos:     99,
		},
		VaultID: uuid.MustParse("33333333-3333-3333-3333-333333333333"),
	}
}

// fullyPopulatedExportRecord returns a proto ExportRecord with every field
// set to a non-zero, distinguishable value.
func fullyPopulatedExportRecord() *gastrologv1.ExportRecord {
	// Use the same Record's ChunkID/IngesterID in their serialized forms.
	rec := fullyPopulatedRecord()
	ingesterID := rec.EventID.IngesterID
	return &gastrologv1.ExportRecord{
		Raw:        []byte("log line: something happened"),
		SourceTs:   timestamppb.New(time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)),
		IngestTs:   timestamppb.New(time.Date(2025, 6, 15, 10, 30, 1, 0, time.UTC)),
		WriteTs:    timestamppb.New(time.Date(2025, 6, 15, 10, 30, 2, 0, time.UTC)),
		Attrs:      map[string]string{"host": "web-1", "level": "error"},
		VaultId:    rec.VaultID.String(),
		ChunkId:    rec.Ref.ChunkID.String(), // base32hex, not UUID format
		Pos:        99,
		IngestSeq:  42,
		IngesterId: ingesterID[:],
	}
}

func TestRecordToExport_AllFields(t *testing.T) {
	rec := fullyPopulatedRecord()
	er := RecordToExport(rec)

	if string(er.Raw) != string(rec.Raw) {
		t.Errorf("Raw: got %q, want %q", er.Raw, rec.Raw)
	}
	if er.SourceTs.AsTime() != rec.SourceTS {
		t.Errorf("SourceTs: got %v, want %v", er.SourceTs.AsTime(), rec.SourceTS)
	}
	if er.IngestTs.AsTime() != rec.IngestTS {
		t.Errorf("IngestTs: got %v, want %v", er.IngestTs.AsTime(), rec.IngestTS)
	}
	if er.WriteTs.AsTime() != rec.WriteTS {
		t.Errorf("WriteTs: got %v, want %v", er.WriteTs.AsTime(), rec.WriteTS)
	}
	if er.VaultId != rec.VaultID.String() {
		t.Errorf("VaultId: got %q, want %q", er.VaultId, rec.VaultID.String())
	}
	if er.ChunkId != rec.Ref.ChunkID.String() {
		t.Errorf("ChunkId: got %q, want %q", er.ChunkId, rec.Ref.ChunkID.String())
	}
	if er.Pos != rec.Ref.Pos {
		t.Errorf("Pos: got %d, want %d", er.Pos, rec.Ref.Pos)
	}
	if er.IngestSeq != rec.EventID.IngestSeq {
		t.Errorf("IngestSeq: got %d, want %d", er.IngestSeq, rec.EventID.IngestSeq)
	}
	var gotIngesterID uuid.UUID
	copy(gotIngesterID[:], er.IngesterId)
	if gotIngesterID != rec.EventID.IngesterID {
		t.Errorf("IngesterId: got %x, want %x", er.IngesterId, rec.EventID.IngesterID[:])
	}
	if er.Attrs["host"] != "web-1" || er.Attrs["level"] != "error" {
		t.Errorf("Attrs: got %v, want host=web-1, level=error", er.Attrs)
	}
}

func TestExportToRecord_AllFields(t *testing.T) {
	er := fullyPopulatedExportRecord()
	rec := ExportToRecord(er)

	if string(rec.Raw) != string(er.Raw) {
		t.Errorf("Raw: got %q, want %q", rec.Raw, er.Raw)
	}
	if !rec.SourceTS.Equal(er.SourceTs.AsTime()) {
		t.Errorf("SourceTS: got %v, want %v", rec.SourceTS, er.SourceTs.AsTime())
	}
	if !rec.IngestTS.Equal(er.IngestTs.AsTime()) {
		t.Errorf("IngestTS: got %v, want %v", rec.IngestTS, er.IngestTs.AsTime())
	}
	if !rec.WriteTS.Equal(er.WriteTs.AsTime()) {
		t.Errorf("WriteTS: got %v, want %v", rec.WriteTS, er.WriteTs.AsTime())
	}
	if rec.VaultID.String() != er.VaultId {
		t.Errorf("VaultID: got %q, want %q", rec.VaultID, er.VaultId)
	}
	if rec.Ref.ChunkID.String() != er.ChunkId {
		t.Errorf("ChunkID: got %q, want %q", rec.Ref.ChunkID, er.ChunkId)
	}
	if rec.Ref.Pos != er.Pos {
		t.Errorf("Pos: got %d, want %d", rec.Ref.Pos, er.Pos)
	}
	if rec.EventID.IngestSeq != er.IngestSeq {
		t.Errorf("IngestSeq: got %d, want %d", rec.EventID.IngestSeq, er.IngestSeq)
	}
	if rec.EventID.IngesterID != uuid.MustParse("11111111-1111-1111-1111-111111111111") {
		t.Errorf("IngesterID: got %v, want 11111111-...", rec.EventID.IngesterID)
	}
	if rec.EventID.IngestTS != rec.IngestTS {
		t.Errorf("EventID.IngestTS: got %v, want %v (should match IngestTS)", rec.EventID.IngestTS, rec.IngestTS)
	}
	if rec.Attrs["host"] != "web-1" || rec.Attrs["level"] != "error" {
		t.Errorf("Attrs: got %v, want host=web-1, level=error", rec.Attrs)
	}
}

func TestRoundTrip_RecordToExportToRecord(t *testing.T) {
	orig := fullyPopulatedRecord()
	exported := RecordToExport(orig)
	imported := ExportToRecord(exported)

	if string(imported.Raw) != string(orig.Raw) {
		t.Errorf("Raw: round-trip mismatch")
	}
	if !imported.SourceTS.Equal(orig.SourceTS) {
		t.Errorf("SourceTS: got %v, want %v", imported.SourceTS, orig.SourceTS)
	}
	if !imported.IngestTS.Equal(orig.IngestTS) {
		t.Errorf("IngestTS: got %v, want %v", imported.IngestTS, orig.IngestTS)
	}
	if !imported.WriteTS.Equal(orig.WriteTS) {
		t.Errorf("WriteTS: got %v, want %v", imported.WriteTS, orig.WriteTS)
	}
	if imported.VaultID != orig.VaultID {
		t.Errorf("VaultID: got %v, want %v", imported.VaultID, orig.VaultID)
	}
	if imported.Ref.ChunkID != orig.Ref.ChunkID {
		t.Errorf("ChunkID: got %v, want %v", imported.Ref.ChunkID, orig.Ref.ChunkID)
	}
	if imported.Ref.Pos != orig.Ref.Pos {
		t.Errorf("Pos: got %d, want %d", imported.Ref.Pos, orig.Ref.Pos)
	}
	if imported.EventID.IngestSeq != orig.EventID.IngestSeq {
		t.Errorf("IngestSeq: got %d, want %d", imported.EventID.IngestSeq, orig.EventID.IngestSeq)
	}
	if imported.EventID.IngesterID != orig.EventID.IngesterID {
		t.Errorf("IngesterID: got %v, want %v", imported.EventID.IngesterID, orig.EventID.IngesterID)
	}
	if len(imported.Attrs) != len(orig.Attrs) {
		t.Fatalf("Attrs length: got %d, want %d", len(imported.Attrs), len(orig.Attrs))
	}
	for k, v := range orig.Attrs {
		if imported.Attrs[k] != v {
			t.Errorf("Attrs[%q]: got %q, want %q", k, imported.Attrs[k], v)
		}
	}
}

func TestRoundTrip_ExportToRecordToExport(t *testing.T) {
	orig := fullyPopulatedExportRecord()
	imported := ExportToRecord(orig)
	reexported := RecordToExport(imported)

	if string(reexported.Raw) != string(orig.Raw) {
		t.Errorf("Raw: round-trip mismatch")
	}
	if reexported.SourceTs.AsTime() != orig.SourceTs.AsTime() {
		t.Errorf("SourceTs: got %v, want %v", reexported.SourceTs.AsTime(), orig.SourceTs.AsTime())
	}
	if reexported.IngestTs.AsTime() != orig.IngestTs.AsTime() {
		t.Errorf("IngestTs: got %v, want %v", reexported.IngestTs.AsTime(), orig.IngestTs.AsTime())
	}
	if reexported.WriteTs.AsTime() != orig.WriteTs.AsTime() {
		t.Errorf("WriteTs: got %v, want %v", reexported.WriteTs.AsTime(), orig.WriteTs.AsTime())
	}
	if reexported.VaultId != orig.VaultId {
		t.Errorf("VaultId: got %q, want %q", reexported.VaultId, orig.VaultId)
	}
	if reexported.ChunkId != orig.ChunkId {
		t.Errorf("ChunkId: got %q, want %q", reexported.ChunkId, orig.ChunkId)
	}
	if reexported.Pos != orig.Pos {
		t.Errorf("Pos: got %d, want %d", reexported.Pos, orig.Pos)
	}
	if reexported.IngestSeq != orig.IngestSeq {
		t.Errorf("IngestSeq: got %d, want %d", reexported.IngestSeq, orig.IngestSeq)
	}
}

func TestRecordToExport_ZeroRecord(t *testing.T) {
	rec := chunk.Record{Raw: []byte("bare log")}
	er := RecordToExport(rec)

	if string(er.Raw) != "bare log" {
		t.Errorf("Raw: got %q, want %q", er.Raw, "bare log")
	}
	if er.SourceTs != nil {
		t.Error("zero SourceTS should produce nil SourceTs")
	}
	if er.IngestTs != nil {
		t.Error("zero IngestTS should produce nil IngestTs")
	}
	if er.WriteTs != nil {
		t.Error("zero WriteTS should produce nil WriteTs")
	}
	if len(er.Attrs) != 0 {
		t.Errorf("empty attrs should produce empty map, got %v", er.Attrs)
	}
}

func TestExportToRecord_ZeroExportRecord(t *testing.T) {
	er := &gastrologv1.ExportRecord{Raw: []byte("bare log")}
	rec := ExportToRecord(er)

	if string(rec.Raw) != "bare log" {
		t.Errorf("Raw: got %q, want %q", rec.Raw, "bare log")
	}
	if !rec.SourceTS.IsZero() {
		t.Error("nil SourceTs should produce zero SourceTS")
	}
	if !rec.IngestTS.IsZero() {
		t.Error("nil IngestTs should produce zero IngestTS")
	}
	if !rec.WriteTS.IsZero() {
		t.Error("nil WriteTs should produce zero WriteTS")
	}
	if rec.VaultID != uuid.Nil {
		t.Errorf("empty VaultId should produce uuid.Nil, got %v", rec.VaultID)
	}
	if rec.EventID.IngestSeq != 0 {
		t.Errorf("IngestSeq: got %d, want 0", rec.EventID.IngestSeq)
	}
}

func TestRecordToExport_AttrsCopied(t *testing.T) {
	rec := chunk.Record{
		Raw:   []byte("test"),
		Attrs: chunk.Attributes{"key": "original"},
	}
	er := RecordToExport(rec)

	// Mutate the original — the export should be independent.
	rec.Attrs["key"] = "mutated"
	if er.Attrs["key"] != "original" {
		t.Error("RecordToExport must copy attrs, not share the map")
	}
}

func TestExportToRecord_AttrsCopied(t *testing.T) {
	er := &gastrologv1.ExportRecord{
		Raw:   []byte("test"),
		Attrs: map[string]string{"key": "original"},
	}
	rec := ExportToRecord(er)

	// Mutate the original — the import should be independent.
	er.Attrs["key"] = "mutated"
	if rec.Attrs["key"] != "original" {
		t.Error("ExportToRecord must copy attrs, not share the map")
	}
}

func TestExportToRecord_ShortIngesterID(t *testing.T) {
	er := &gastrologv1.ExportRecord{
		Raw:        []byte("test"),
		IngesterId: []byte{1, 2, 3}, // too short — should be ignored
		IngestSeq:  5,
	}
	rec := ExportToRecord(er)

	if rec.EventID.IngesterID != (uuid.UUID{}) {
		t.Errorf("short IngesterId should produce zero UUID, got %v", rec.EventID.IngesterID)
	}
	if rec.EventID.IngestSeq != 5 {
		t.Errorf("IngestSeq should still be set even with short IngesterId: got %d", rec.EventID.IngestSeq)
	}
}

// FuzzRecordRoundTrip fuzzes the Record → Export → Record round-trip.
// Any chunk.Record that survives the export path should produce a
// semantically equivalent record when imported back.
func FuzzRecordRoundTrip(f *testing.F) {
	f.Add(
		[]byte("round trip log"),
		int64(1705312200), int64(1705312201), int64(1705312202),
		"app", "api-server",
		uint32(7),
	)
	f.Add([]byte{}, int64(0), int64(0), int64(0), "", "", uint32(0))
	f.Add([]byte("x"), int64(-1), int64(-1), int64(-1), "k", "v", uint32(0xFFFF))

	f.Fuzz(func(t *testing.T, raw []byte, srcSec, ingSec, writeSec int64, attrKey, attrVal string, ingestSeq uint32) {
		rec := chunk.Record{Raw: raw}
		if srcSec != 0 {
			rec.SourceTS = time.Unix(srcSec, 0)
		}
		if ingSec != 0 {
			rec.IngestTS = time.Unix(ingSec, 0)
		}
		if writeSec != 0 {
			rec.WriteTS = time.Unix(writeSec, 0)
		}
		if attrKey != "" {
			rec.Attrs = chunk.Attributes{attrKey: attrVal}
		}
		rec.EventID.IngestSeq = ingestSeq

		exported := RecordToExport(rec)
		imported := ExportToRecord(exported)

		if string(imported.Raw) != string(rec.Raw) {
			t.Errorf("raw mismatch after round-trip")
		}
		if attrKey != "" && imported.Attrs[attrKey] != attrVal {
			t.Errorf("attr %q mismatch: got %q, want %q", attrKey, imported.Attrs[attrKey], attrVal)
		}
		if imported.EventID.IngestSeq != ingestSeq {
			t.Errorf("IngestSeq mismatch: got %d, want %d", imported.EventID.IngestSeq, ingestSeq)
		}
	})
}

// FuzzExportToRecord fuzzes ExportToRecord with arbitrary proto inputs.
// Must not panic on any input — malicious or corrupt records from the wire
// must be handled gracefully.
func FuzzExportToRecord(f *testing.F) {
	f.Add(
		[]byte("log line"),
		int64(1705312200), int32(0),
		int64(1705312201), int32(500000000),
		"host", "web-1",
		uint32(42),
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	)
	f.Add([]byte{}, int64(0), int32(0), int64(0), int32(0), "", "", uint32(0), []byte{})
	f.Add([]byte("neg"), int64(-1), int32(-1), int64(-1), int32(-1), "k", "v", uint32(1), []byte{0xff})

	f.Fuzz(func(t *testing.T, raw []byte, srcSec int64, srcNano int32, ingSec int64, ingNano int32, attrKey, attrVal string, ingestSeq uint32, ingesterID []byte) {
		er := &gastrologv1.ExportRecord{Raw: raw}
		if srcSec != 0 || srcNano != 0 {
			er.SourceTs = &timestamppb.Timestamp{Seconds: srcSec, Nanos: srcNano}
		}
		if ingSec != 0 || ingNano != 0 {
			er.IngestTs = &timestamppb.Timestamp{Seconds: ingSec, Nanos: ingNano}
		}
		if attrKey != "" {
			er.Attrs = map[string]string{attrKey: attrVal}
		}
		er.IngestSeq = ingestSeq
		er.IngesterId = ingesterID

		// Must not panic.
		rec := ExportToRecord(er)

		if len(raw) > 0 && len(rec.Raw) != len(raw) {
			t.Errorf("raw length mismatch: got %d, want %d", len(rec.Raw), len(raw))
		}
	})
}
