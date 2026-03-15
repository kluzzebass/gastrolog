package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// FuzzExportRecordToChunk fuzzes the exportRecordToChunk conversion function.
// This function runs on the receiving node when processing ForwardRecords and
// ForwardImportRecords RPCs — it converts incoming proto ExportRecords to
// internal chunk.Records. Malicious or corrupt records must not cause panics.
func FuzzExportRecordToChunk(f *testing.F) {
	// Seed: typical record with all fields populated.
	f.Add(
		[]byte("2024-01-15T10:30:00Z some log line with content"),
		int64(1705312200), // source_ts seconds
		int32(0),          // source_ts nanos
		int64(1705312201), // ingest_ts seconds
		int32(500000000),  // ingest_ts nanos
		"host",            // attr key
		"web-1",           // attr value
		uint32(42),        // ingest_seq
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, // ingester_id (16 bytes)
	)

	// Seed: empty record.
	f.Add([]byte{}, int64(0), int32(0), int64(0), int32(0), "", "", uint32(0), []byte{})

	// Seed: record with no timestamps.
	f.Add([]byte("bare log"), int64(0), int32(0), int64(0), int32(0), "", "", uint32(0), []byte{})

	// Seed: record with oversized raw payload.
	bigRaw := make([]byte, 65536)
	for i := range bigRaw {
		bigRaw[i] = byte(i % 256)
	}
	f.Add(bigRaw, int64(1e12), int32(999999999), int64(1e12), int32(999999999), "env", "prod", uint32(0xFFFFFFFF), make([]byte, 32))

	// Seed: negative timestamp.
	f.Add([]byte("neg"), int64(-1), int32(-1), int64(-1), int32(-1), "k", "v", uint32(1), []byte{0xff})

	f.Fuzz(func(t *testing.T, raw []byte, srcSec int64, srcNano int32, ingSec int64, ingNano int32, attrKey, attrVal string, ingestSeq uint32, ingesterID []byte) {
		er := &gastrologv1.ExportRecord{
			Raw: raw,
		}

		// Only set timestamps if seconds are non-zero to test both paths.
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
		rec := exportRecordToChunk(er)

		// Basic sanity: Raw bytes should round-trip.
		if len(raw) > 0 && len(rec.Raw) != len(raw) {
			t.Errorf("raw length mismatch: got %d, want %d", len(rec.Raw), len(raw))
		}
	})
}

// FuzzExportRecordRoundTrip fuzzes the chunkRecordToExport -> exportRecordToChunk
// round-trip. Any chunk.Record that survives the export path should produce a
// semantically equivalent record when imported back.
func FuzzExportRecordRoundTrip(f *testing.F) {
	f.Add([]byte("round trip log"), int64(1705312200), int64(1705312201), "app", "api-server", uint32(7))
	f.Add([]byte{}, int64(0), int64(0), "", "", uint32(0))
	f.Add([]byte("x"), int64(-1), int64(-1), "k", "v", uint32(0xFFFF))

	f.Fuzz(func(t *testing.T, raw []byte, srcSec, ingSec int64, attrKey, attrVal string, ingestSeq uint32) {
		// Build a chunk.Record.
		rec := chunkRecordForFuzz(raw, srcSec, ingSec, attrKey, attrVal)

		// Export it.
		exported := chunkRecordToExport(rec)
		exported.IngestSeq = ingestSeq

		// Import it back — must not panic.
		imported := exportRecordToChunk(exported)

		// Verify raw bytes survived.
		if string(imported.Raw) != string(rec.Raw) {
			t.Errorf("raw mismatch after round-trip")
		}

		// Verify attrs survived (if present).
		if attrKey != "" {
			if imported.Attrs[attrKey] != attrVal {
				t.Errorf("attr %q mismatch: got %q, want %q", attrKey, imported.Attrs[attrKey], attrVal)
			}
		}
	})
}

func chunkRecordForFuzz(raw []byte, srcSec, ingSec int64, attrKey, attrVal string) chunk.Record {
	var rec chunk.Record
	rec.Raw = raw
	if srcSec != 0 {
		rec.SourceTS = time.Unix(srcSec, 0)
	}
	if ingSec != 0 {
		rec.IngestTS = time.Unix(ingSec, 0)
	}
	if attrKey != "" {
		rec.Attrs = chunk.Attributes{attrKey: attrVal}
	}
	return rec
}
