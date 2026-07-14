package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/manifest"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/query"
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
// (gastrolog-54mjat) — and that reverse reads return the same record at the
// same position as forward iteration.
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
