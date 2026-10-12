package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/query"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ingestMixedSourceTimes ingests n records one second apart in ingest time.
// Every third record has no source timestamp; the others carry source times
// running backwards over the same seconds, so records without a source
// timestamp interleave with, and tie against, sourced ones on the source axis.
func (o *originFixture) ingestMixedSourceTimes(t *testing.T, ctx context.Context, n int) {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingester := glid.New()
	for i := range n {
		ingestTS := t0.Add(time.Duration(i) * time.Second)
		rec := &record.Record{
			EventID:  record.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			IngestTS: ingestTS,
			Raw:      fmt.Appendf(nil, "mixed-source-%03d", i),
		}
		if i%3 != 0 {
			rec.SourceTS = t0.Add(time.Duration(n-1-i) * time.Second)
		}
		o.ingest(t, ctx, rec)
	}
}

// sourceOrdered returns recs' payloads in canonical source order computed
// here: SourceTS, or IngestTS for a record without one, then EventID.
func sourceOrdered(recs []chunk.Record, reverse bool) []string {
	sorted := slices.Clone(recs)
	key := func(r chunk.Record) time.Time {
		if r.SourceTS.IsZero() {
			return r.IngestTS
		}
		return r.SourceTS
	}
	slices.SortFunc(sorted, func(a, b chunk.Record) int {
		c := key(a).Compare(key(b))
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

func payloads(recs []chunk.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Raw)
	}
	return out
}

// pageSourceOrder pages a source-ordered search through the engine's own
// resume tokens.
func pageSourceOrder(t *testing.T, eng *query.Engine, q query.Query, limit int) []string {
	t.Helper()
	q.OrderBy = query.OrderBySourceTS
	q.Limit = limit
	var out []string
	var token *query.ResumeToken
	for page := 0; page < 1000; page++ {
		it, next := eng.Search(context.Background(), q, token)
		for rec, err := range it {
			if err != nil {
				t.Fatalf("page %d: %v", page, err)
			}
			out = append(out, string(rec.Raw))
		}
		if token = next(); token == nil {
			return out
		}
	}
	t.Fatal("paging never finished")
	return nil
}

// A chunk's records come back in the same source order while the chunk is
// open, sorted in memory, and once it is sealed, walked through the STSI its
// pipeline build wrote: a record without a source timestamp sits at its
// ingest timestamp both times, with source windows and paging agreeing too.
func TestPipelineSourceOrderAgreesAcrossSeal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 30
	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	origin.ingestMixedSourceTimes(t, ctx, n)
	home := t.TempDir()
	var total uint64
	for _, e := range origin.waitAllPublished(t) {
		copyCompletedToHead(t, origin.root, home, e.SegmentID)
		total += uint64(e.RecordCount)
	}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunkingSpec(home, fsm, func() bool { return true })); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	open := planUntil(t, ctx, mgr, fsm, vaultID, fmt.Sprintf("open manifest covering the %d published records", total), func(open *vaultctlfsm.OpenChunkManifest) bool {
		return open.TotalRecords >= total
	})
	if open.TotalRecords != n {
		t.Fatalf("open manifest holds %d records, want %d", open.TotalRecords, n)
	}

	openCM, openIM := newQueryCM(t)
	openEng := query.NewWithRegistry(&pipelineSearchRegistry{
		vaultID: vaultID, cm: openCM, im: openIM, home: home, fsm: fsm,
		metas: []chunk.ChunkMeta{openChunkManifestToChunkMeta(open, chunk.ChunkStateActive)},
	}, nil)
	all := drainSearch(t, openEng, query.Query{})
	if len(all) != n {
		t.Fatalf("open chunk serves %d records, want %d", len(all), n)
	}

	lo, hi := time.Date(2025, 6, 15, 10, 0, 8, 0, time.UTC), time.Date(2025, 6, 15, 10, 0, 21, 0, time.UTC)
	var windowed []chunk.Record
	for _, r := range all {
		ts := r.SourceTS
		if ts.IsZero() {
			ts = r.IngestTS
		}
		if !ts.Before(lo) && ts.Before(hi) {
			windowed = append(windowed, r)
		}
	}

	type view struct {
		name string
		q    query.Query
		want func(reverse bool) []string
	}
	views := []view{
		{"whole chunk", query.Query{}, func(reverse bool) []string { return sourceOrdered(all, reverse) }},
		{"source window", query.Query{SourceStart: lo, SourceEnd: hi}, func(reverse bool) []string { return sourceOrdered(windowed, reverse) }},
	}
	check := func(state string, eng *query.Engine) {
		t.Helper()
		for _, v := range views {
			for _, reverse := range []bool{false, true} {
				q := v.q
				q.IsReverse = reverse
				want := v.want(reverse)
				q.OrderBy = query.OrderBySourceTS
				if got := payloads(drainSearch(t, eng, q)); !slices.Equal(got, want) {
					t.Errorf("%s, %s, reverse=%v:\n got  %v\n want %v", state, v.name, reverse, got, want)
				}
				if got := pageSourceOrder(t, eng, q, 4); !slices.Equal(got, want) {
					t.Errorf("%s, %s, reverse=%v, paged by 4:\n got  %v\n want %v", state, v.name, reverse, got, want)
				}
			}
		}
	}
	check("open", openEng)

	if err := mgr.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("RotateCron: %v", err)
	}
	if sm := fsm.SealedManifest(); sm == nil || sm.ChunkID != open.ChunkID {
		t.Fatal("the open chunk was not the one sealed")
	}
	if err := mgr.BuildOnce(ctx, vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	var sealed vaultctlfsm.ManifestEntry
	for _, e := range fsm.List() {
		if e.ID == open.ChunkID && e.IsSealed() {
			sealed = e
		}
	}
	if sealed.ID != open.ChunkID {
		t.Fatal("sealed entry not found in FSM")
	}
	sealedCM, sealedIM := newQueryCM(t)
	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), open.ChunkID)
	if err := sealedCM.RegisterExternalGLCB(open.ChunkID, glcbPath, externalInfoFromEntry(sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	if stsiLen, err := sealedIM.SourceIndexLen(open.ChunkID); err != nil || stsiLen != uint64(len(all)) {
		t.Fatalf("sealed STSI len = (%d, %v), want (%d, nil)", stsiLen, err, len(all))
	}
	check("sealed", query.New(sealedCM, sealedIM, nil))
}
