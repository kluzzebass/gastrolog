package query_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/format"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

var fallbackBase = time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

// sealedEngine registers each record set as one sealed GLCB chunk and returns
// an engine over them with the blob paths in chunk order.
func sealedEngine(t *testing.T, chunks ...[]chunk.Record) (*query.Engine, []string) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	vaultID := glid.New()
	root := t.TempDir()
	paths := make([]string, 0, len(chunks))
	for i, recs := range chunks {
		id := chunk.NewChunkID()
		path, info := glcbtest.WriteSealedBlob(t, root, id, vaultID, recs)
		if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
			t.Fatalf("register chunk %d: %v", i, err)
		}
		paths = append(paths, path)
	}
	return query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil), paths
}

// mixedSourceRecords returns n records 10ms apart in ingest time. Every third
// record has no SourceTS; the rest carry source times running backwards over
// the same span, so a sourced record and a record without a source timestamp
// share a timestamp wherever their positions mirror.
func mixedSourceRecords(n int, ingester glid.GLID) []chunk.Record {
	recs := make([]chunk.Record, 0, n)
	for i := range n {
		ingestTS := fallbackBase.Add(time.Duration(i) * 10 * time.Millisecond)
		rec := chunk.Record{
			IngestTS: ingestTS,
			WriteTS:  ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "r%03d", i),
		}
		if i%3 != 0 {
			rec.SourceTS = fallbackBase.Add(time.Duration(n-1-i) * 10 * time.Millisecond)
		}
		recs = append(recs, rec)
	}
	return recs
}

// sourceAxisTS is the expected source-axis timestamp, written out here rather
// than taken from the code under test.
func sourceAxisTS(rec chunk.Record) time.Time {
	if rec.SourceTS.IsZero() {
		return rec.IngestTS
	}
	return rec.SourceTS
}

// expectedSourceOrder returns the raw payloads of recs in canonical source
// order: source-axis timestamp, then EventID.
func expectedSourceOrder(recs []chunk.Record, reverse bool) []string {
	sorted := slices.Clone(recs)
	slices.SortFunc(sorted, func(a, b chunk.Record) int {
		c := sourceAxisTS(a).Compare(sourceAxisTS(b))
		if c == 0 {
			c = a.EventID.Compare(b.EventID)
		}
		if reverse {
			return -c
		}
		return c
	})
	out := make([]string, len(sorted))
	for i, r := range sorted {
		out[i] = string(r.Raw)
	}
	return out
}

func raws(recs []chunk.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Raw)
	}
	return out
}

func searchSourceOrder(t *testing.T, eng *query.Engine, q query.Query) []chunk.Record {
	t.Helper()
	q.OrderBy = query.OrderBySourceTS
	got, err := collect(search(eng, context.Background(), q))
	if err != nil {
		t.Fatalf("source-ordered search: %v", err)
	}
	return got
}

func assertSameSequence(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s:\n got  %v\n want %v", what, got, want)
	}
}

// split deals recs round-robin into n record sets.
func split(recs []chunk.Record, n int) [][]chunk.Record {
	out := make([][]chunk.Record, n)
	for i, r := range recs {
		out[i%n] = append(out[i%n], r)
	}
	return out
}

// A source-ordered search places a record without a source timestamp by its
// ingest timestamp, in a sealed chunk where no record has one, where some do,
// and where all do; in one chunk or across several; in both directions; and
// paging through it in small pages loses and repeats nothing across the
// boundary between sourced records and the ones that fall back.
func TestSourceOrderPlacesRecordsWithoutSourceTSByIngestTS(t *testing.T) {
	ingester := glid.New()
	none := mixedSourceRecords(24, ingester)
	for i := range none {
		none[i].SourceTS = time.Time{}
	}
	all := mixedSourceRecords(24, ingester)
	for i := range all {
		if all[i].SourceTS.IsZero() {
			all[i].SourceTS = all[i].IngestTS.Add(-time.Hour)
		}
	}
	for _, tc := range []struct {
		name string
		recs []chunk.Record
	}{
		{"no record has a source timestamp", none},
		{"some records have a source timestamp", mixedSourceRecords(24, ingester)},
		{"every record has a source timestamp", all},
	} {
		for _, chunks := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/%d chunks", tc.name, chunks), func(t *testing.T) {
				eng, _ := sealedEngine(t, split(tc.recs, chunks)...)
				stored := make(map[string]time.Time, len(tc.recs))
				for _, r := range tc.recs {
					stored[string(r.Raw)] = r.SourceTS
				}
				for _, reverse := range []bool{false, true} {
					want := expectedSourceOrder(tc.recs, reverse)
					got := searchSourceOrder(t, eng, query.Query{IsReverse: reverse})
					assertSameSequence(t, fmt.Sprintf("reverse=%v", reverse), raws(got), want)
					for _, rec := range got {
						if !rec.SourceTS.Equal(stored[string(rec.Raw)]) {
							t.Fatalf("%s served SourceTS %v, stored %v", rec.Raw, rec.SourceTS, stored[string(rec.Raw)])
						}
					}
					paged := pageThrough(t, eng, query.Query{OrderBy: query.OrderBySourceTS, IsReverse: reverse}, 5)
					assertSameSequence(t, fmt.Sprintf("reverse=%v paged by 5", reverse), raws(paged), want)
				}
			})
		}
	}
}

// A source window keeps the records whose source-axis timestamp falls in it:
// a record without a source timestamp by its ingest timestamp, a sourced
// record by its source timestamp even when its ingest timestamp is inside.
func TestSourceWindowFiltersRecordsWithoutSourceTSByIngestTS(t *testing.T) {
	recs := mixedSourceRecords(24, glid.New())
	lo, hi := fallbackBase.Add(50*time.Millisecond), fallbackBase.Add(120*time.Millisecond)
	var inWindow []chunk.Record
	fallbackIn, sourcedOutByIngestIn := 0, 0
	for _, r := range recs {
		ts := sourceAxisTS(r)
		in := !ts.Before(lo) && ts.Before(hi)
		if in {
			inWindow = append(inWindow, r)
			if r.SourceTS.IsZero() {
				fallbackIn++
			}
		}
		if !in && !r.SourceTS.IsZero() && !r.IngestTS.Before(lo) && r.IngestTS.Before(hi) {
			sourcedOutByIngestIn++
		}
	}
	if fallbackIn == 0 || sourcedOutByIngestIn == 0 {
		t.Fatalf("fixture proves nothing: %d fallback records inside, %d sourced records outside with ingest inside", fallbackIn, sourcedOutByIngestIn)
	}
	eng, _ := sealedEngine(t, split(recs, 2)...)
	for _, reverse := range []bool{false, true} {
		got := searchSourceOrder(t, eng, query.Query{SourceStart: lo, SourceEnd: hi, IsReverse: reverse})
		assertSameSequence(t, fmt.Sprintf("reverse=%v", reverse), raws(got), expectedSourceOrder(inWindow, reverse))
	}
	// The filter is the same rule under the default ordering.
	got, err := collect(search(eng, context.Background(), query.Query{SourceStart: lo, SourceEnd: hi}))
	if err != nil {
		t.Fatal(err)
	}
	gotSet, wantSet := raws(got), expectedSourceOrder(inWindow, false)
	slices.Sort(gotSet)
	slices.Sort(wantSet)
	assertSameSequence(t, "ingest-ordered search with a source window", gotSet, wantSet)
}

// A chunk whose source timestamps all lie outside a source window still holds
// the window's records when its records without a source timestamp were
// ingested inside it, so the window must not skip the chunk.
func TestSourceWindowKeepsChunkWhoseOnlyMatchesFallBack(t *testing.T) {
	ingester := glid.New()
	var recs []chunk.Record
	for i := range 6 {
		ingestTS := fallbackBase.Add(time.Duration(i) * time.Second)
		rec := chunk.Record{
			IngestTS: ingestTS,
			WriteTS:  ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "k%d", i),
		}
		if i%2 == 0 {
			rec.SourceTS = ingestTS.Add(-time.Hour)
		}
		recs = append(recs, rec)
	}
	eng, _ := sealedEngine(t, recs)
	q := query.Query{SourceStart: fallbackBase, SourceEnd: fallbackBase.Add(time.Minute)}
	got := searchSourceOrder(t, eng, q)
	assertSameSequence(t, "source window over the ingest span", raws(got), []string{"k1", "k3", "k5"})
}

// A chunk whose records without a source timestamp precede, on the source
// axis, every record of another chunk opens first: a limited source-ordered
// search returns them, not the other chunk's earliest record.
func TestSourceOrderOpensTheChunkWhoseFallbackRecordsComeFirst(t *testing.T) {
	ingester := glid.New()
	rec := func(seq int, ingestTS, sourceTS time.Time, raw string) chunk.Record {
		return chunk.Record{
			SourceTS: sourceTS,
			IngestTS: ingestTS,
			WriteTS:  ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(seq)}, //nolint:gosec // G115: small test sequence
			Raw:      []byte(raw),
		}
	}
	at := func(s int) time.Time { return fallbackBase.Add(time.Duration(s) * time.Second) }
	early := []chunk.Record{
		rec(1, at(1), time.Time{}, "fallback@1"),
		rec(2, at(20), at(10), "source@10"),
		rec(3, at(21), at(11), "source@11"),
	}
	late := []chunk.Record{
		rec(4, at(22), at(5), "source@5"),
		rec(5, at(23), at(6), "source@6"),
	}
	all := slices.Concat(early, late)
	for _, reverse := range []bool{false, true} {
		eng, _ := sealedEngine(t, early, late)
		want := expectedSourceOrder(all, reverse)
		got := searchSourceOrder(t, eng, query.Query{IsReverse: reverse, Limit: 1})
		assertSameSequence(t, fmt.Sprintf("reverse=%v limit=1", reverse), raws(got), want[:1])
		got = searchSourceOrder(t, eng, query.Query{IsReverse: reverse})
		assertSameSequence(t, fmt.Sprintf("reverse=%v", reverse), raws(got), want)
	}
}

// rewriteSourceIndexTOCEntry applies edit to the STSI entry in the TOC at the
// tail of the GLCB at path.
func rewriteSourceIndexTOCEntry(t *testing.T, path string, edit func(entry []byte)) {
	t.Helper()
	const footerSize, entrySize = 44, 42
	data, err := os.ReadFile(path) //nolint:gosec // G304: test-owned temp path
	if err != nil {
		t.Fatal(err)
	}
	footer := data[len(data)-footerSize:]
	count := int(binary.LittleEndian.Uint32(footer[0:4]))
	entries := data[len(data)-footerSize-count*entrySize : len(data)-footerSize]
	edited := false
	for i := range count {
		entry := entries[i*entrySize : (i+1)*entrySize]
		if entry[0] == glcb.SectionSourceTSIndex {
			edit(entry)
			edited = true
		}
	}
	if !edited {
		t.Fatal("no STSI entry in the TOC")
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// An STSI the TOC does not list, or one whose bytes are not whole entries,
// fails a source-ordered search loudly: only an index that is present and
// empty means "no entries".
func TestSourceOrderFailsOnMissingOrCorruptSourceIndex(t *testing.T) {
	for _, tc := range []struct {
		name    string
		edit    func(entry []byte)
		wantErr func(error) bool
	}{
		{
			name:    "missing",
			edit:    func(entry []byte) { entry[0] = format.TypeTokenIndex },
			wantErr: func(err error) bool { return errors.Is(err, glcb.ErrSectionNotFound) },
		},
		{
			name: "not whole entries",
			edit: func(entry []byte) {
				binary.LittleEndian.PutUint32(entry[6:10], binary.LittleEndian.Uint32(entry[6:10])-1)
			},
			wantErr: func(err error) bool { return err != nil && strings.Contains(err.Error(), "not a multiple") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, paths := sealedEngine(t, mixedSourceRecords(12, glid.New()))
			rewriteSourceIndexTOCEntry(t, paths[0], tc.edit)
			got, err := collect(search(eng, context.Background(), query.Query{OrderBy: query.OrderBySourceTS}))
			if !tc.wantErr(err) {
				t.Fatalf("source-ordered search over a %s STSI: %d records, err=%v", tc.name, len(got), err)
			}
			if ingest, err := collect(search(eng, context.Background(), query.Query{})); err != nil || len(ingest) != 12 {
				t.Fatalf("ingest-ordered search over the same chunk: %d records, err=%v", len(ingest), err)
			}
		})
	}
}

// A source-ordered timechart that scans records bins one without a source
// timestamp at its ingest timestamp, so every record lands in a bucket.
func TestSourceOrderTimechartBinsRecordsWithoutSourceTSByIngestTS(t *testing.T) {
	recs := mixedSourceRecords(24, glid.New())
	for i := range recs {
		recs[i].Attrs = chunk.Attributes{"kind": "x"}
	}
	eng, _ := sealedEngine(t, split(recs, 2)...)
	const buckets = 4
	start, end := fallbackBase, fallbackBase.Add(240*time.Millisecond)
	width := end.Sub(start) / buckets
	want := make([]int64, buckets)
	for _, r := range recs {
		want[sourceAxisTS(r).Sub(start)/width]++
	}

	pipeline, err := querylang.ParsePipeline(fmt.Sprintf("kind=x | timechart %d", buckets))
	if err != nil {
		t.Fatal(err)
	}
	result, err := eng.RunPipeline(context.Background(), query.Query{BoolExpr: pipeline.Filter, OrderBy: query.OrderBySourceTS, Start: start, End: end}, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, 0, buckets)
	for _, row := range result.Table.Rows {
		n, err := strconv.ParseInt(row[1], 10, 64)
		if err != nil {
			t.Fatalf("count %q: %v", row[1], err)
		}
		got = append(got, n)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("bucket counts = %v, want %v", got, want)
	}
}
