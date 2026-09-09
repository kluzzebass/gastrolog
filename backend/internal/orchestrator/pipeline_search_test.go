package orchestrator

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/manifest"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/query"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// buildOpenPipelineManifest ingests records and drives the planner until the
// open manifest covers them, without sealing.
func buildOpenPipelineManifest(t *testing.T, ctx context.Context) (glid.GLID, *vaultctlfsm.FSM, string, chunk.ChunkID, uint64) {
	t.Helper()
	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAndPublish(t, ctx)

	home := t.TempDir()
	copyCompletedToHead(t, origin.root, home, segID)

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunkingSpec(home, fsm, func() bool { return true })); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var open *vaultctlfsm.OpenChunkManifest
	for {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
		open = fsm.OpenChunk()
		if open != nil && len(open.Refs) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("open manifest never opened")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("chunk must remain open")
	}
	return vaultID, fsm, home, open.ChunkID, open.TotalRecords
}

// buildOpenPipelineManifestN is buildOpenPipelineManifest with enough records
// for paging tests: it ingests n records, waits for the origin to publish what
// it will, brings every completed segment to head, and plans until the open
// manifest references all of them.
func buildOpenPipelineManifestN(t *testing.T, ctx context.Context, n int) (glid.GLID, *vaultctlfsm.FSM, string, chunk.ChunkID, uint64) {
	t.Helper()
	return buildOpenPipelineManifestWith(t, ctx, func(origin *originFixture) {
		origin.ingestAttributed(t, ctx, n, "rubicon-c-segment-payload-line-for-paging-tests", nil)
	})
}

// ingestScrambledSourceTimes ingests n records one second apart in ingest
// time whose source times are a permutation of those seconds, so source order
// disagrees with write order throughout the chunk.
func (o *originFixture) ingestScrambledSourceTimes(t *testing.T, ctx context.Context, n int) {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingester := glid.New()
	for i := range n {
		ingestTS := t0.Add(time.Duration(i) * time.Second)
		rec := record.Record{
			EventID:  record.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			IngestTS: ingestTS,
			SourceTS: t0.Add(time.Duration((i*7)%n) * time.Second),
			Raw:      []byte("rubicon-c-segment-payload-line-with-scrambled-source-times"),
		}
		ack := make(chan error, 1)
		select {
		case o.in <- segmentation.Input{Record: &rec, Ack: ack}:
		case <-ctx.Done():
			t.Fatal("ingest cancelled")
		}
		select {
		case err := <-ack:
			if err != nil {
				t.Fatalf("ingest ack: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ingest ack timeout")
		}
	}
}

// buildOpenPipelineManifestWith runs ingest against a fresh origin, waits for
// the origin to publish what it will, brings every completed segment to head,
// and plans until the open manifest references all of them.
func buildOpenPipelineManifestWith(t *testing.T, ctx context.Context, ingest func(*originFixture)) (glid.GLID, *vaultctlfsm.FSM, string, chunk.ChunkID, uint64) {
	t.Helper()
	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	ingest(origin)

	// Segments publish asynchronously; settle on the set that arrives.
	var entries []vaultctlfsm.CompletedSegmentEntry
	stable := time.Now()
	for time.Now().Before(stable.Add(500 * time.Millisecond)) {
		cur := fsm.ListCompletedSegments()
		if len(cur) != len(entries) {
			entries = cur
			stable = time.Now()
		}
		time.Sleep(5 * time.Millisecond)
	}
	var total uint64
	home := t.TempDir()
	for _, e := range entries {
		copyCompletedToHead(t, origin.root, home, e.SegmentID)
		total += uint64(e.RecordCount)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunkingSpec(home, fsm, func() bool { return true })); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var open *vaultctlfsm.OpenChunkManifest
	for {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
		open = fsm.OpenChunk()
		if open != nil && open.TotalRecords >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("open manifest never covered the %d published records", total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("chunk must remain open")
	}
	return vaultID, fsm, home, open.ChunkID, open.TotalRecords
}

type pipelineSearchRegistry struct {
	vaultID glid.GLID
	cm      chunk.ChunkManager
	im      index.IndexManager
	metas   []chunk.ChunkMeta
	home    string
	fsm     *vaultctlfsm.FSM
}

func (r *pipelineSearchRegistry) ListVaults() []glid.GLID { return []glid.GLID{r.vaultID} }

func (r *pipelineSearchRegistry) ChunkManager(id glid.GLID) chunk.ChunkManager {
	if id == r.vaultID {
		return r.cm
	}
	return nil
}

func (r *pipelineSearchRegistry) IndexManager(id glid.GLID) index.IndexManager {
	if id == r.vaultID {
		return r.im
	}
	return nil
}

func (r *pipelineSearchRegistry) Reader() manifest.Reader { return r }

func (r *pipelineSearchRegistry) Entry(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
	return vaultctlfsm.ManifestEntry{}, false
}

func (r *pipelineSearchRegistry) EntriesForVault(glid.GLID) []vaultctlfsm.ManifestEntry {
	return nil
}

func (r *pipelineSearchRegistry) IndexReader() manifest.IndexReader { return e1NoIndexReader{} }

func (r *pipelineSearchRegistry) SearchChunkMetas(vaultID glid.GLID) []chunk.ChunkMeta {
	if vaultID == r.vaultID {
		return r.metas
	}
	return nil
}

func (r *pipelineSearchRegistry) OpenPipelineChunkCursor(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	if vaultID != r.vaultID {
		return nil, chunk.ErrChunkNotFound
	}
	open := r.fsm.OpenChunk()
	if open == nil || open.ChunkID != chunkID {
		if sm := r.fsm.SealedManifest(); sm != nil && sm.ChunkID == chunkID {
			open = sm
		} else {
			return nil, chunk.ErrChunkNotFound
		}
	}
	locate := chunking.VaultSegmentLocator{Root: r.home}
	seq, _, err := chunking.QueryOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: open,
		Locate:   locate,
	})
	if err != nil {
		return nil, err
	}
	openReader := func() (*chunking.OpenChunkReader, error) {
		reader, _, err := chunking.NewOpenChunkReader(chunking.OpenChunkQueryInput{
			Manifest: open,
			Locate:   locate,
		})
		return reader, err
	}
	return newManifestRecordCursor(chunkID, seq, open.TotalRecords, openReader), nil
}

func (r *pipelineSearchRegistry) ScanPipelineChunkIngestTS(vaultID glid.GLID, chunkID chunk.ChunkID, cb func(tsNanos int64) bool) error {
	cursor, err := r.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return err
		}
		if !cb(rec.IngestTS.UnixNano()) {
			return nil
		}
	}
}

// TestPipelineActiveChunkSearchable: active pipeline chunks appear in search
// chunk discovery and stream records via manifest segment spans.
func TestPipelineActiveChunkSearchable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vaultID, fsm, home, chunkID, wantRecords := buildOpenPipelineManifest(t, ctx)
	open := fsm.OpenChunk()
	meta := openChunkManifestToChunkMeta(open, chunk.ChunkStateActive)

	cm, im := newQueryCM(t)
	reg := &pipelineSearchRegistry{
		vaultID: vaultID,
		cm:      cm,
		im:      im,
		metas:   []chunk.ChunkMeta{meta},
		home:    home,
		fsm:     fsm,
	}

	eng := query.NewWithRegistry(reg, nil)
	got := drainSearch(t, eng, query.Query{})
	if uint64(len(got)) != wantRecords {
		t.Fatalf("search returned %d records, want %d", len(got), wantRecords)
	}
	for _, rec := range got {
		if len(rec.Raw) == 0 {
			t.Fatal("expected non-empty payload")
		}
	}
	_ = chunkID
}

func TestManifestRecordCursorReverseSeek(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vaultID, fsm, home, chunkID, wantRecords := buildOpenPipelineManifest(t, ctx)
	open := fsm.OpenChunk()
	if wantRecords < 2 {
		t.Fatalf("need >= 2 records for reverse test, got %d", wantRecords)
	}

	reg := &pipelineSearchRegistry{vaultID: vaultID, home: home, fsm: fsm}
	cursor, err := reg.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cursor.Close() }()

	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: open.TotalRecords}); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	rec, _, err := cursor.Prev()
	if err != nil {
		t.Fatalf("Prev: %v", err)
	}
	if len(rec.Raw) == 0 {
		t.Fatal("expected non-empty last record")
	}

	// Forward from start should still work after reverse positioning.
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: 0}); err != nil {
		t.Fatalf("Seek start: %v", err)
	}
	rec, _, err = cursor.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(rec.Raw) == 0 {
		t.Fatal("expected non-empty first record")
	}
}

// TestManifestRecordCursorReverseNoReopenAndForwardParity asserts, via
// open-count instrumentation (not timing), that a full reverse scan over an
// active pipeline chunk performs ZERO full-verify segment opens and exactly
// one mapped open per distinct manifest segment for the whole cursor lifetime
// — and that reverse reads return the same record at the same position as
// forward iteration.
func TestManifestRecordCursorReverseNoReopenAndForwardParity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vaultID, fsm, home, chunkID, wantRecords := buildOpenPipelineManifest(t, ctx)
	open := fsm.OpenChunk()
	if wantRecords < 2 {
		t.Fatalf("need >= 2 records for reverse test, got %d", wantRecords)
	}
	distinctSegs := map[glid.GLID]struct{}{}
	for _, ref := range open.Refs {
		distinctSegs[ref.SegmentID] = struct{}{}
	}

	reg := &pipelineSearchRegistry{vaultID: vaultID, home: home, fsm: fsm}

	// Forward pass: position -> record via Next.
	fwdCursor, err := reg.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		t.Fatal(err)
	}
	var forward []chunk.Record
	for {
		rec, _, err := fwdCursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		forward = append(forward, rec)
	}
	if err := fwdCursor.Close(); err != nil {
		t.Fatalf("close forward cursor: %v", err)
	}
	if uint64(len(forward)) != wantRecords {
		t.Fatalf("forward records = %d, want %d", len(forward), wantRecords)
	}

	cursor, err := reg.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cursor.Close() }()

	opensBefore := segment.Opens()
	mappedBefore := segment.MappedOpens()

	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: open.TotalRecords}); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	var reverse []chunk.Record
	for {
		rec, _, err := cursor.Prev()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatalf("Prev: %v", err)
		}
		reverse = append(reverse, rec)
	}

	if delta := segment.Opens() - opensBefore; delta != 0 {
		t.Fatalf("reverse scan made %d full-verify segment.Open calls, want 0", delta)
	}
	if delta := segment.MappedOpens() - mappedBefore; delta != uint64(len(distinctSegs)) {
		t.Fatalf("reverse scan made %d OpenMapped calls, want %d (one per distinct segment)",
			delta, len(distinctSegs))
	}

	if len(reverse) != len(forward) {
		t.Fatalf("reverse records = %d, want %d", len(reverse), len(forward))
	}
	for i, rec := range reverse {
		want := forward[len(forward)-1-i]
		if rec.Ref != want.Ref || string(rec.Raw) != string(want.Raw) {
			t.Fatalf("reverse[%d] = ref %+v raw %q, forward has ref %+v raw %q",
				i, rec.Ref, rec.Raw, want.Ref, want.Raw)
		}
	}
}

func TestPipelineActiveChunkHistogram(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vaultID, fsm, home, chunkID, wantRecords := buildOpenPipelineManifest(t, ctx)
	open := fsm.OpenChunk()
	meta := openChunkManifestToChunkMeta(open, chunk.ChunkStateActive)

	cm, im := newQueryCM(t)
	reg := &pipelineSearchRegistry{
		vaultID: vaultID,
		cm:      cm,
		im:      im,
		metas:   []chunk.ChunkMeta{meta},
		home:    home,
		fsm:     fsm,
	}

	eng := query.NewWithRegistry(reg, nil)
	q := query.Query{
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	buckets := eng.ComputeSearchPageHistogram(ctx, q, 10)
	var total int64
	for _, b := range buckets {
		total += b.Count
	}
	if total != int64(wantRecords) {
		t.Fatalf("histogram total = %d, want %d (active pipeline chunk %s)", total, wantRecords, chunkID)
	}
}

// The cursor contract the engine resumes by, as the file cursor implements
// it: after Seek(p), Next returns the record at p and Prev the record before
// it. A cursor that lands one past p makes the engine's forward resume — seek,
// then one Next to step over the record the previous page ended on — skip the
// first record of every page.
func TestManifestRecordCursorSeekMatchesTheFileCursorContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vaultID, fsm, home, chunkID, wantRecords := buildOpenPipelineManifestN(t, ctx, 60)
	if wantRecords < 8 {
		t.Fatalf("need >= 8 records in the open manifest, got %d", wantRecords)
	}
	reg := &pipelineSearchRegistry{vaultID: vaultID, home: home, fsm: fsm}

	byPos := make(map[uint64]string, wantRecords)
	fwd, err := reg.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		t.Fatal(err)
	}
	for {
		rec, ref, err := fwd.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		byPos[ref.Pos] = string(rec.Raw)
	}
	_ = fwd.Close()

	// Seek(totalRecords) is the end-of-chunk position a reverse scan starts
	// from: Prev returns the last record.
	end, err := reg.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		t.Fatal(err)
	}
	if err := end.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: wantRecords}); err != nil {
		t.Fatal(err)
	}
	if _, ref, err := end.Prev(); err != nil || ref.Pos != wantRecords-1 {
		t.Errorf("Seek(end) then Prev returned position %d (err %v), want %d", ref.Pos, err, wantRecords-1)
	}
	_ = end.Close()

	for _, p := range []uint64{0, 1, wantRecords / 2, wantRecords - 1} {
		cursor, err := reg.OpenPipelineChunkCursor(vaultID, chunkID)
		if err != nil {
			t.Fatal(err)
		}
		// Force the positional reader, the branch a resume takes.
		if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: p}); err != nil {
			t.Fatalf("Seek(%d): %v", p, err)
		}
		rec, ref, err := cursor.Next()
		if err != nil {
			t.Fatalf("Seek(%d) then Next: %v", p, err)
		}
		if ref.Pos != p || string(rec.Raw) != byPos[p] {
			t.Errorf("Seek(%d) then Next returned position %d", p, ref.Pos)
		}
		if p+1 < wantRecords {
			if _, ref, err := cursor.Next(); err != nil || ref.Pos != p+1 {
				t.Errorf("Seek(%d), Next, Next returned position %d (err %v), want %d", p, ref.Pos, err, p+1)
			}
		} else if _, _, err := cursor.Next(); !errors.Is(err, chunk.ErrNoMoreRecords) {
			t.Errorf("Seek(last), Next, Next: want exhaustion, got %v", err)
		}
		if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: p}); err != nil {
			t.Fatalf("Seek(%d): %v", p, err)
		}
		rec, ref, err = cursor.Prev()
		switch {
		case p == 0:
			if !errors.Is(err, chunk.ErrNoMoreRecords) {
				t.Errorf("Seek(0) then Prev returned position %d, want exhaustion", ref.Pos)
			}
		case err != nil:
			t.Fatalf("Seek(%d) then Prev: %v", p, err)
		case ref.Pos != p-1 || string(rec.Raw) != byPos[p-1]:
			t.Errorf("Seek(%d) then Prev returned position %d, want %d", p, ref.Pos, p-1)
		}
		_ = cursor.Close()
	}
}

// Paging through an active pipeline chunk on the node that holds it resumes
// by position. Every record must arrive exactly once, in both directions.
func TestPipelineActiveChunkPagesWithoutLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vaultID, fsm, home, _, wantRecords := buildOpenPipelineManifestN(t, ctx, 60)
	if wantRecords < 8 {
		t.Fatalf("need >= 8 records in the open manifest, got %d", wantRecords)
	}
	open := fsm.OpenChunk()
	meta := openChunkManifestToChunkMeta(open, chunk.ChunkStateActive)
	cm, im := newQueryCM(t)
	reg := &pipelineSearchRegistry{vaultID: vaultID, cm: cm, im: im, metas: []chunk.ChunkMeta{meta}, home: home, fsm: fsm}
	eng := query.NewWithRegistry(reg, nil)

	for _, reverse := range []bool{false, true} {
		seen := make(map[uint64]int, wantRecords)
		var token *query.ResumeToken
		for page := 0; page < int(wantRecords)+2; page++ {
			it, next := eng.Search(ctx, query.Query{Limit: 3, IsReverse: reverse}, token)
			n := 0
			for rec, err := range it {
				if err != nil {
					t.Fatalf("reverse=%v page %d: %v", reverse, page, err)
				}
				seen[rec.Ref.Pos]++
				n++
			}
			token = next()
			if token == nil {
				break
			}
			if n == 0 {
				t.Fatalf("reverse=%v page %d returned nothing but a token", reverse, page)
			}
		}
		var lost, dup []uint64
		for p := uint64(0); p < wantRecords; p++ {
			switch seen[p] {
			case 0:
				lost = append(lost, p)
			case 1:
			default:
				dup = append(dup, p)
			}
		}
		if len(lost) > 0 || len(dup) > 0 {
			t.Errorf("reverse=%v: paged %d of %d records; lost %v, duplicated %v", reverse, len(seen), wantRecords, lost, dup)
		}
	}
}

// An open pipeline chunk has no source index, so under order=source_ts the
// engine must sort it: every record in canonical source order, forward and
// reverse, and paging through it must lose and repeat nothing.
func TestPipelineActiveChunkSourceOrderIsSorted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 60
	vaultID, fsm, home, _, wantRecords := buildOpenPipelineManifestWith(t, ctx, func(origin *originFixture) {
		origin.ingestScrambledSourceTimes(t, ctx, n)
	})
	if wantRecords < 8 {
		t.Fatalf("need >= 8 records in the open manifest, got %d", wantRecords)
	}
	open := fsm.OpenChunk()
	meta := openChunkManifestToChunkMeta(open, chunk.ChunkStateActive)
	cm, im := newQueryCM(t)
	reg := &pipelineSearchRegistry{vaultID: vaultID, cm: cm, im: im, metas: []chunk.ChunkMeta{meta}, home: home, fsm: fsm}
	eng := query.NewWithRegistry(reg, nil)

	for _, reverse := range []bool{false, true} {
		got := drainSearch(t, eng, query.Query{OrderBy: query.OrderBySourceTS, IsReverse: reverse})
		if uint64(len(got)) != wantRecords {
			t.Fatalf("reverse=%v: %d records, want %d", reverse, len(got), wantRecords)
		}
		scrambled := 0
		for i := 1; i < len(got); i++ {
			if got[i].IngestTS.Before(got[i-1].IngestTS) != reverse {
				scrambled++
			}
			if a, b := got[i-1].SourceTS, got[i].SourceTS; (!reverse && b.Before(a)) || (reverse && b.After(a)) {
				t.Fatalf("reverse=%v: out of source order at %d: %v then %v", reverse, i, a, b)
			}
		}
		if scrambled == 0 {
			t.Fatalf("reverse=%v: source order coincides with write order; the fixture proves nothing", reverse)
		}

		seen := make(map[uint64]int, wantRecords)
		var token *query.ResumeToken
		for page := 0; page < int(wantRecords)+2; page++ {
			it, next := eng.Search(ctx, query.Query{OrderBy: query.OrderBySourceTS, IsReverse: reverse, Limit: 7}, token)
			for rec, err := range it {
				if err != nil {
					t.Fatalf("reverse=%v page %d: %v", reverse, page, err)
				}
				seen[rec.Ref.Pos]++
			}
			if token = next(); token == nil {
				break
			}
		}
		for p := uint64(0); p < wantRecords; p++ {
			if seen[p] != 1 {
				t.Errorf("reverse=%v: position %d seen %d times", reverse, p, seen[p])
			}
		}
	}
}

// A position handed out while a chunk is open must name the same record once
// the chunk is sealed: the open-chunk cursor and the sealed blob are two
// readers of one chunk, and every resume token, context fetch and record
// reference that spans a seal depends on them agreeing.
func TestOpenChunkPositionsSurviveSealing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 40
	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	origin.ingestScrambledSourceTimes(t, ctx, n)
	home := t.TempDir()
	var entries []vaultctlfsm.CompletedSegmentEntry
	stable := time.Now()
	for time.Now().Before(stable.Add(500 * time.Millisecond)) {
		if cur := fsm.ListCompletedSegments(); len(cur) != len(entries) {
			entries, stable = cur, time.Now()
		}
		time.Sleep(5 * time.Millisecond)
	}
	var total uint64
	for _, e := range entries {
		copyCompletedToHead(t, origin.root, home, e.SegmentID)
		total += uint64(e.RecordCount)
	}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunkingSpec(home, fsm, func() bool { return true })); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var open *vaultctlfsm.OpenChunkManifest
	for {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
		if open = fsm.OpenChunk(); open != nil && open.TotalRecords >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("open manifest never covered the %d published records", total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if open.TotalRecords < 8 {
		t.Fatalf("open manifest holds %d records, need >= 8", open.TotalRecords)
	}

	// Positions while open, through the cursor a search uses.
	reg := &pipelineSearchRegistry{vaultID: vaultID, home: home, fsm: fsm}
	cursor, err := reg.OpenPipelineChunkCursor(vaultID, open.ChunkID)
	if err != nil {
		t.Fatal(err)
	}
	openByPos := make(map[uint64]string)
	for {
		rec, ref, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		openByPos[ref.Pos] = string(rec.EventID.IngestTS.Format(time.RFC3339Nano)) + " " + string(rec.Raw)
	}
	_ = cursor.Close()
	if _, ok := openByPos[0]; !ok {
		t.Fatalf("open chunk positions do not start at 0: %v", openByPos)
	}

	// Seal and build the same chunk.
	if err := mgr.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("RotateCron: %v", err)
	}
	sealedManifest := fsm.SealedManifest()
	if sealedManifest == nil || sealedManifest.ChunkID != open.ChunkID {
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
	cm, im := newQueryCM(t)
	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), open.ChunkID)
	if err := cm.RegisterExternalGLCB(open.ChunkID, glcbPath, externalInfoFromEntry(sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	eng := query.New(cm, im, nil)
	sealedByPos := make(map[uint64]string)
	for _, rec := range drainSearch(t, eng, query.Query{}) {
		sealedByPos[rec.Ref.Pos] = string(rec.EventID.IngestTS.Format(time.RFC3339Nano)) + " " + string(rec.Raw)
	}

	if len(sealedByPos) != len(openByPos) {
		t.Fatalf("sealed chunk serves %d records, open served %d", len(sealedByPos), len(openByPos))
	}
	for pos, want := range openByPos {
		if got := sealedByPos[pos]; got != want {
			t.Errorf("position %d: open %q, sealed %q", pos, want, got)
		}
	}
}
