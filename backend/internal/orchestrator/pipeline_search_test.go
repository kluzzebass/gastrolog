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
	readAt := func(pos uint64) (chunk.Record, error) {
		rec, err := chunking.ReadManifestRecordAt(open, locate, pos)
		if err != nil {
			return chunk.Record{}, err
		}
		cr := chunking.RecordToChunk(rec)
		cr.Ref = chunk.RecordRef{ChunkID: chunkID, Pos: pos}
		return cr, nil
	}
	return newManifestRecordCursor(chunkID, seq, open.TotalRecords, readAt), nil
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
