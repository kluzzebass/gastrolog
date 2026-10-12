package query_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb/glcbtest"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

// boundFixture builds an engine over one chunk holding recs in the given
// physical order.
type boundFixture struct {
	name  string
	build func(t *testing.T, recs []chunk.Record) *query.Engine
	// sourceOrder is false where the chunk has no source-order rank view and
	// a source-ordered search is refused.
	sourceOrder bool
	// postingLists is true where a token query is served from the token
	// index's posting lists rather than a per-record token filter.
	postingLists bool
}

var boundFixtures = []boundFixture{
	{"sealed GLCB", sealedGLCBEngine, true, false},
	{"sealed with token index", sealedMemoryEngine, false, true},
	{"active memory chunk", activeMemoryEngine, false, false},
	{"active file chunk", activeFileEngine, false, false},
}

func sealedGLCBEngine(t *testing.T, recs []chunk.Record) *query.Engine {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, glid.New(), recs)
	if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("register: %v", err)
	}
	return query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil)
}

func sealedMemoryEngine(t *testing.T, recs []chunk.Record) *query.Engine {
	t.Helper()
	v := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(10000)})
	for _, rec := range recs {
		if _, _, err := v.CM.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := v.CM.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	memtest.BuildIndexes(t, v.CM, v.IM)
	return v.QE
}

func activeMemoryEngine(t *testing.T, recs []chunk.Record) *query.Engine {
	t.Helper()
	v := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(10000)})
	for _, rec := range recs {
		if _, _, err := v.CM.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	return v.QE
}

func activeFileEngine(t *testing.T, recs []chunk.Record) *query.Engine {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	for _, rec := range recs {
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	return query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil)
}

// boundRecords places one record per offset at consecutive physical
// positions. The shuffled axis carries base+offset; the other axis steps one
// second per position. Every record holds the token "needle".
func boundRecords(base time.Time, offsets []time.Duration, ingestShuffled, sourceShuffled bool) []chunk.Record {
	ingester := glid.New()
	recs := make([]chunk.Record, 0, len(offsets))
	for i, off := range offsets {
		inOrder := base.Add(time.Duration(i) * time.Second)
		ingestTS, sourceTS := inOrder, inOrder
		if ingestShuffled {
			ingestTS = base.Add(off)
		}
		if sourceShuffled {
			sourceTS = base.Add(off)
		}
		recs = append(recs, chunk.Record{
			SourceTS: sourceTS,
			IngestTS: ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "p%d needle", i),
		})
	}
	return recs
}

// admitted names the positions whose offset is at or above bound.
func admitted(offsets []time.Duration, bound time.Duration) []string {
	var out []string
	for i, off := range offsets {
		if off >= bound {
			out = append(out, fmt.Sprintf("p%d", i))
		}
	}
	return out
}

func searchPositions(t *testing.T, eng *query.Engine, q query.Query) []string {
	t.Helper()
	it, _ := eng.Search(context.Background(), q, nil)
	out := []string{}
	for rec, err := range it {
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		name, _, _ := strings.Cut(string(rec.Raw), " ")
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

// shuffledOffsets puts the earliest timestamps in the middle of the chunk and
// repeats the in-window boundary value, so a bound at +3s admits positions
// 0, 2, 3 and 5 — the lowest of them ahead of records the bound excludes.
var shuffledOffsets = []time.Duration{10 * time.Second, 0, 5 * time.Second, 3 * time.Second, time.Second, 3 * time.Second}

// boundCases are lower bounds before every record, inside the chunk on an
// existing timestamp, between timestamps, and past every record.
var boundCases = []time.Duration{-time.Second, 3 * time.Second, 4 * time.Second, 11 * time.Second}

type boundAxis struct {
	name                           string
	ingestShuffled, sourceShuffled bool
	withBound                      func(q query.Query, at time.Time) query.Query
}

var boundAxes = []boundAxis{
	{"start", true, false, func(q query.Query, at time.Time) query.Query { q.Start = at; return q }},
	{"source_start", false, true, func(q query.Query, at time.Time) query.Query { q.SourceStart = at; return q }},
}

// A lower time bound admits every record at or above it wherever the record
// sits in the chunk. Records reach a chunk out of timestamp order (late,
// replayed or multi-source logs), so the first admitted record can sit
// behind records the bound excludes.
func TestLowerBoundKeepsRecordsOutOfPhysicalOrder(t *testing.T) {
	t.Parallel()
	base := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	for _, fx := range boundFixtures {
		for _, axis := range boundAxes {
			eng := fx.build(t, boundRecords(base, shuffledOffsets, axis.ingestShuffled, axis.sourceShuffled))
			if fx.postingLists {
				requireTokenIndexed(t, eng, query.Query{Tokens: []string{"needle"}})
			}
			for _, order := range []query.OrderBy{query.OrderByIngestTS, query.OrderBySourceTS} {
				if order == query.OrderBySourceTS && !fx.sourceOrder {
					continue
				}
				for _, reverse := range []bool{false, true} {
					for _, tokens := range [][]string{nil, {"needle"}} {
						for _, bound := range boundCases {
							name := fmt.Sprintf("%s/%s/order=%s/reverse=%v/tokens=%v/bound=%s", fx.name, axis.name, order, reverse, tokens, bound)
							q := axis.withBound(query.Query{OrderBy: order, IsReverse: reverse, Tokens: tokens}, base.Add(bound))
							want := admitted(shuffledOffsets, bound)
							if got := searchPositions(t, eng, q); !slices.Equal(got, want) {
								t.Errorf("%s: got %v, want %v", name, got, want)
							}
						}
					}
				}
			}
		}
	}
}

// monotonicOffsets never decrease and repeat the boundary value, so a bound
// at +2s excludes exactly the first two positions.
var monotonicOffsets = []time.Duration{0, time.Second, 2 * time.Second, 2 * time.Second, 2 * time.Second, 3 * time.Second}

// On a sealed chunk whose IngestTS never decreases in physical order, the
// records an ingest lower bound excludes are exactly the leading positions,
// so the scan starts past them, and still returns every admitted record.
func TestIngestLowerBoundNarrowsMonotonicSealedChunk(t *testing.T) {
	t.Parallel()
	base := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	for _, fx := range boundFixtures[:2] {
		eng := fx.build(t, boundRecords(base, monotonicOffsets, true, false))
		for _, bound := range []time.Duration{-time.Second, 2 * time.Second, 3 * time.Second} {
			for _, order := range []query.OrderBy{query.OrderByIngestTS, query.OrderBySourceTS} {
				if order == query.OrderBySourceTS && !fx.sourceOrder {
					continue
				}
				for _, reverse := range []bool{false, true} {
					for _, tokens := range [][]string{nil, {"needle"}} {
						q := query.Query{OrderBy: order, IsReverse: reverse, Tokens: tokens, Start: base.Add(bound)}
						want := admitted(monotonicOffsets, bound)
						if got := searchPositions(t, eng, q); !slices.Equal(got, want) {
							t.Errorf("%s/order=%s/reverse=%v/tokens=%v/bound=%s: got %v, want %v", fx.name, order, reverse, tokens, bound, got, want)
						}
					}
				}
			}
			skipped := len(monotonicOffsets) - len(admitted(monotonicOffsets, bound))
			if got, ok := ingestSeekSkip(t, eng, query.Query{Start: base.Add(bound)}); !ok || got != skipped {
				t.Errorf("%s/bound=%s: ingest seek skips %d (present=%v), want %d", fx.name, bound, got, ok, skipped)
			}
		}
	}
}

// readCountingManager counts the records search reads through its cursors.
type readCountingManager struct {
	*chunkfile.Manager
	reads int
}

func (m *readCountingManager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	c, err := m.Manager.OpenCursor(id)
	if err != nil {
		return nil, err
	}
	return &readCountingCursor{RecordCursor: c, reads: &m.reads}, nil
}

type readCountingCursor struct {
	chunk.RecordCursor
	reads *int
}

func (c *readCountingCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	*c.reads++
	return c.RecordCursor.Next()
}

func (c *readCountingCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	*c.reads++
	return c.RecordCursor.Prev()
}

// A source-ordered walk of a sealed chunk with monotonic IngestTS reads only
// the records at or above the ingest bound's first position; the source rank
// range alone would read every record.
func TestIngestLowerBoundSkipsReadsOnMonotonicSealedChunk(t *testing.T) {
	t.Parallel()
	base := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	dir := t.TempDir()
	inner, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = inner.Close() })
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, glid.New(), boundRecords(base, monotonicOffsets, true, false))
	if err := inner.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("register: %v", err)
	}
	cm := &readCountingManager{Manager: inner}
	eng := query.New(cm, indexfile.NewManager(dir, nil, nil, inner), nil)

	for _, reverse := range []bool{false, true} {
		cm.reads = 0
		q := query.Query{OrderBy: query.OrderBySourceTS, IsReverse: reverse, Start: base.Add(2 * time.Second)}
		want := admitted(monotonicOffsets, 2*time.Second)
		if got := searchPositions(t, eng, q); !slices.Equal(got, want) {
			t.Fatalf("reverse=%v: got %v, want %v", reverse, got, want)
		}
		if cm.reads != len(want) {
			t.Errorf("reverse=%v: read %d records, want %d", reverse, cm.reads, len(want))
		}
	}
}

// Where the chunk's IngestTS is out of physical order, or the chunk is still
// active, no position window can stand for the ingest bound: the plan shows no
// ingest seek and the bound is applied per record.
func TestIngestLowerBoundSkipsSeekWithoutPhysicalOrder(t *testing.T) {
	t.Parallel()
	base := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	cases := []struct {
		name    string
		build   func(t *testing.T, recs []chunk.Record) *query.Engine
		offsets []time.Duration
	}{
		{"sealed GLCB, shuffled ingest", sealedGLCBEngine, shuffledOffsets},
		{"sealed with token index, shuffled ingest", sealedMemoryEngine, shuffledOffsets},
		{"active memory chunk, monotonic ingest", activeMemoryEngine, monotonicOffsets},
		{"active file chunk, monotonic ingest", activeFileEngine, monotonicOffsets},
	}
	for _, tc := range cases {
		eng := tc.build(t, boundRecords(base, tc.offsets, true, false))
		if skip, ok := ingestSeekSkip(t, eng, query.Query{Start: base.Add(3 * time.Second)}); ok {
			t.Errorf("%s: plan seeks past %d positions, want no ingest seek", tc.name, skip)
		}
	}
}

// ingestSeekSkip reports how many leading positions the plan's ingest seek
// skips in the engine's only chunk.
func ingestSeekSkip(t *testing.T, eng *query.Engine, q query.Query) (int, bool) {
	t.Helper()
	plan, err := eng.Explain(context.Background(), q)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(plan.ChunkPlans) != 1 {
		t.Fatalf("explain: %d chunk plans, want 1", len(plan.ChunkPlans))
	}
	cp := plan.ChunkPlans[0]
	for _, step := range cp.Pipeline {
		if step.Index == "ingest_ts" && step.Action == "seek" {
			return cp.RecordCount - step.PositionsAfter, true
		}
	}
	return 0, false
}

// requireTokenIndexed fails the test unless the plan serves the query's
// tokens from posting lists.
func requireTokenIndexed(t *testing.T, eng *query.Engine, q query.Query) {
	t.Helper()
	plan, err := eng.Explain(context.Background(), q)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	for _, cp := range plan.ChunkPlans {
		for _, step := range cp.Pipeline {
			if step.Index == "token" && step.Action == "indexed" {
				return
			}
		}
	}
	t.Fatal("token query is not served from posting lists")
}
