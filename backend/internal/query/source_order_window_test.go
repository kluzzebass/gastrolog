package query_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
)

// offsetSourceEngine builds an engine over sealed chunks whose records were
// ingested at ingestBase onward, one per second, with source timestamps one
// hour earlier — the shape of logs that reach the cluster late.
func offsetSourceEngine(t *testing.T, n int, ingestBase time.Time) *query.Engine {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	vaultID, ingester := glid.New(), glid.New()
	recs := make([]chunk.Record, 0, n)
	for i := range n {
		ingestTS := ingestBase.Add(time.Duration(i) * time.Second)
		recs = append(recs, chunk.Record{
			SourceTS: ingestTS.Add(-time.Hour),
			IngestTS: ingestTS,
			WriteTS:  ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "r%03d", i),
		})
	}
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, vaultID, recs)
	if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("register: %v", err)
	}
	return query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil)
}

func countRecords(t *testing.T, eng *query.Engine, q query.Query) int {
	t.Helper()
	it, _ := eng.Search(context.Background(), q, nil)
	n := 0
	for _, err := range it {
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		n++
	}
	return n
}

// start/end bound the ingest axis whatever the ordering, and source_start/
// source_end bound the source axis. A source-ordered scan through the source
// index must honour that split: an ingest window that covers every record's
// ingest time keeps every record even though their source times fall an hour
// before it, and a source window selects on source time alone.
func TestSourceOrderKeepsIngestAndSourceWindowsApart(t *testing.T) {
	const n = 20
	ingestBase := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	eng := offsetSourceEngine(t, n, ingestBase)
	ingestWindow := query.Query{Start: ingestBase, End: ingestBase.Add(time.Hour)}

	tests := []struct {
		name string
		q    query.Query
		want int
	}{
		{"default order, ingest window", ingestWindow, n},
		{"source order, no window", query.Query{OrderBy: query.OrderBySourceTS}, n},
		{"source order, ingest window", query.Query{OrderBy: query.OrderBySourceTS, Start: ingestWindow.Start, End: ingestWindow.End}, n},
		{"source order, reverse, ingest window", query.Query{OrderBy: query.OrderBySourceTS, IsReverse: true, Start: ingestWindow.Start, End: ingestWindow.End}, n},
		{"source order, ingest window admits half", query.Query{OrderBy: query.OrderBySourceTS, Start: ingestBase.Add(10 * time.Second), End: ingestWindow.End}, n / 2},
		{"source order, source window admits half", query.Query{OrderBy: query.OrderBySourceTS, SourceStart: ingestBase.Add(-time.Hour + 10*time.Second)}, n / 2},
		{"source order, both windows", query.Query{OrderBy: query.OrderBySourceTS, Start: ingestBase.Add(5 * time.Second), End: ingestWindow.End, SourceStart: ingestBase.Add(-time.Hour + 10*time.Second)}, n / 2},
		{"source order, source window above every source time", query.Query{OrderBy: query.OrderBySourceTS, SourceStart: ingestBase}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := countRecords(t, eng, tc.q); got != tc.want {
				t.Errorf("%s: %d records, want %d", tc.name, got, tc.want)
			}
		})
	}
}
