package query_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

func TestHistogramQuietBucketsZeroAfterIngestStops(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 6, 24, 21, 8, 0, 0, time.UTC)
	const ingestSecs = 80

	v := memtest.MustNewVault(t, chunkmem.Config{})
	for i := range ingestSecs * 100 {
		ts := t0.Add(time.Duration(i) * time.Second / 100)
		if _, _, err := v.CM.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.CM.Seal(); err != nil {
		t.Fatal(err)
	}
	memtest.BuildIndexes(t, v.CM, v.IM)

	q := query.Query{Start: t0, End: t0.Add(5 * time.Minute)}
	buckets := v.QE.ComputeHistogram(context.Background(), q, 50)
	if len(buckets) == 0 {
		t.Fatal("expected histogram buckets")
	}

	ingestEnd := t0.Add(ingestSecs * time.Second)
	for _, b := range buckets {
		bucketStart := time.UnixMilli(b.TimestampMs)
		if !bucketStart.Before(ingestEnd) && b.Count != 0 {
			t.Errorf("quiet bucket %v count = %d, want 0", bucketStart, b.Count)
		}
	}
}
