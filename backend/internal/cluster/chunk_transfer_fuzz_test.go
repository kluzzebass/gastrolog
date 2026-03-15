package cluster

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// FuzzChunkRecordToExport fuzzes the chunkRecordToExport conversion used
// during chunk transfers between nodes. The function converts internal
// chunk.Records to proto ExportRecords for wire transmission. It must
// handle any valid Record without panicking.
func FuzzChunkRecordToExport(f *testing.F) {
	f.Add(
		[]byte("log line from chunk transfer"),
		int64(1705312200), // source_ts unix
		int64(1705312201), // ingest_ts unix
		int64(1705312202), // write_ts unix
		"host",            // attr key
		"db-primary",      // attr value
	)

	f.Add([]byte{}, int64(0), int64(0), int64(0), "", "")
	f.Add([]byte("x"), int64(-1000), int64(-2000), int64(-3000), "", "")
	f.Add(make([]byte, 8192), int64(1e15), int64(1e15), int64(1e15), "very-long-key-name-that-exceeds-normal-expectations", "very-long-value")

	f.Fuzz(func(t *testing.T, raw []byte, srcSec, ingSec, writeSec int64, attrKey, attrVal string) {
		rec := chunk.Record{
			Raw:      raw,
			SourceTS: time.Unix(srcSec, 0),
			IngestTS: time.Unix(ingSec, 0),
			WriteTS:  time.Unix(writeSec, 0),
		}

		if attrKey != "" {
			rec.Attrs = chunk.Attributes{attrKey: attrVal}
		}

		// chunkRecordToExport must not panic on any input.
		exported := chunkRecordToExport(rec)

		// Verify raw bytes are preserved.
		if string(exported.Raw) != string(raw) {
			t.Errorf("raw bytes not preserved: got %d bytes, want %d", len(exported.Raw), len(raw))
		}

		// Verify attrs are preserved when present.
		if attrKey != "" {
			if exported.Attrs == nil {
				t.Fatal("attrs map is nil when key was provided")
			}
			if exported.Attrs[attrKey] != attrVal {
				t.Errorf("attr %q: got %q, want %q", attrKey, exported.Attrs[attrKey], attrVal)
			}
		}

		// Verify zero timestamps produce nil proto timestamps.
		zeroRec := chunk.Record{Raw: []byte("z")}
		zeroExported := chunkRecordToExport(zeroRec)
		if zeroExported.SourceTs != nil {
			t.Error("zero SourceTS should produce nil SourceTs proto field")
		}
		if zeroExported.IngestTs != nil {
			t.Error("zero IngestTS should produce nil IngestTs proto field")
		}

		// Round-trip: export then import must not panic.
		_ = exportRecordToChunk(exported)
	})
}
