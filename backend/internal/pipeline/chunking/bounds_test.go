package chunking_test

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/record"
)

// TestSliceRecordBoundsMatchesRecordScan pins the view-based bounds scan
// against a reference computation over fully-materialized records: the
// zero-copy path must produce identical min/max timestamps.
func TestSliceRecordBoundsMatchesRecordScan(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	// Deliberately non-monotonic SourceTS/IngestTS so min ≠ first, max ≠ last.
	recs := []record.Record{
		makeRecord(0, base.Add(5*time.Second), "r0"),
		makeRecord(1, base, "r1"),
		makeRecord(2, base.Add(9*time.Second), "r2"),
		makeRecord(3, base.Add(2*time.Second), "r3"),
	}
	path := writeSegment(t, segID, vaultID, recs)

	idx, err := chunking.BuildOrderedIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	got, err := chunking.SliceRecordBounds(idx, 0, uint32(len(recs)-1))
	if err != nil {
		t.Fatal(err)
	}

	// Reference: same bounds from full records.
	var wantIngestStart, wantIngestEnd, wantSourceStart, wantSourceEnd, wantWriteStart, wantWriteEnd time.Time
	for pos := range uint32(len(recs)) {
		rec, err := idx.RecordAt(pos)
		if err != nil {
			t.Fatal(err)
		}
		if wantIngestStart.IsZero() || rec.IngestTS.Before(wantIngestStart) {
			wantIngestStart = rec.IngestTS
		}
		if rec.IngestTS.After(wantIngestEnd) {
			wantIngestEnd = rec.IngestTS
		}
		if wantSourceStart.IsZero() || rec.SourceTS.Before(wantSourceStart) {
			wantSourceStart = rec.SourceTS
		}
		if rec.SourceTS.After(wantSourceEnd) {
			wantSourceEnd = rec.SourceTS
		}
		if wantWriteStart.IsZero() || rec.WriteTS.Before(wantWriteStart) {
			wantWriteStart = rec.WriteTS
		}
		if rec.WriteTS.After(wantWriteEnd) {
			wantWriteEnd = rec.WriteTS
		}
	}
	if !got.IngestStart.Equal(wantIngestStart) || !got.IngestEnd.Equal(wantIngestEnd) {
		t.Fatalf("ingest bounds = [%v, %v], want [%v, %v]", got.IngestStart, got.IngestEnd, wantIngestStart, wantIngestEnd)
	}
	if !got.SourceStart.Equal(wantSourceStart) || !got.SourceEnd.Equal(wantSourceEnd) {
		t.Fatalf("source bounds = [%v, %v], want [%v, %v]", got.SourceStart, got.SourceEnd, wantSourceStart, wantSourceEnd)
	}
	if !got.WriteStart.Equal(wantWriteStart) || !got.WriteEnd.Equal(wantWriteEnd) {
		t.Fatalf("write bounds = [%v, %v], want [%v, %v]", got.WriteStart, got.WriteEnd, wantWriteStart, wantWriteEnd)
	}

	// Sub-slice bounds exclude records outside the slice.
	sub, err := chunking.SliceRecordBounds(idx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if sub.SourceStart.Equal(got.SourceStart) && sub.SourceEnd.Equal(got.SourceEnd) {
		t.Fatal("sub-slice bounds identical to full-slice bounds; slice limits ignored")
	}
}

// BenchmarkSliceRecordBounds measures the planner's bounds pass — the scan
// that ran hot enough on a loaded home to be its largest allocation source
// (gastrolog-11y2iv increment 3).
func BenchmarkSliceRecordBounds(b *testing.B) {
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	const n = 512
	recs := make([]record.Record, n)
	for i := range recs {
		recs[i] = makeRecord(uint32(i), base.Add(time.Duration(i)*time.Millisecond), "benchmark-record-body")
	}
	path := writeSegment(b, segID, vaultID, recs)

	idx, err := chunking.BuildOrderedIndex(path)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := chunking.SliceRecordBounds(idx, 0, n-1); err != nil {
			b.Fatal(err)
		}
	}
}
