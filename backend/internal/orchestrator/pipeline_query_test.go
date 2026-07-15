package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/manifest"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/query"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// These tests exercise Rubicon E1: a sealed GLCB built by the pipeline (under
// the vault's segmentation ChunkRoot) becomes queryable through query.Engine
// once the orchestrator registers it with the local chunk Manager by path
// (chunk.ExternalGLCBRegistrar). Discovery already works off the vault-ctl FSM;
// E1 closes the byte-access gap. We build a real GLCB with the same wiring the
// chunking lifecycle tests use (originFixture + chunking leader over a shared
// FSM), then drive query.Engine.Search directly.
//
// The embedded-TS-index fast path is intentionally NOT exercised here: the
// index manager resolves ITSI/STSI from its own dir, not the external
// ChunkRoot, so query falls back to the (correct) reorder-buffer + sequential
// scan path. Index-sidecar parity is deferred per the E1 plan.

const e1Payload = "needle-pipeline-record"

// ingestAttributed feeds n records (each carrying raw + attrs at increasing
// IngestTS) through the origin, then waits for the first completed segment to
// be published to the FSM and returns its ID. attrs may be nil for the
// empty-attribute case (guards the Rubicon D decode fix end-to-end).
func (o *originFixture) ingestAttributed(t *testing.T, ctx context.Context, n int, raw string, attrs record.Attributes) glid.GLID {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Second)
		rec := record.Record{
			EventID:  record.EventID{IngestTS: ts},
			IngestTS: ts,
			Raw:      []byte(raw),
			Attrs:    attrs,
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

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if entries := o.fsm.ListCompletedSegments(); len(entries) >= 1 {
			return entries[0].SegmentID
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("origin did not publish a completed segment to the FSM")
	return glid.GLID{}
}

// sealedGLCBFixture is a built pipeline chunk: the sealed FSM entry plus the
// absolute path to the data.glcb the leader produced under home/chunks.
type sealedGLCBFixture struct {
	vaultID  glid.GLID
	fsm      *vaultctlfsm.FSM
	sealed   vaultctlfsm.ManifestEntry
	glcbPath string
}

// buildSealedPipelineGLCB runs the origin → chunking-leader path to produce one
// sealed GLCB and returns its FSM entry and on-disk path. n/raw/attrs control
// the ingested records (attrs nil = empty-attribute records).
func buildSealedPipelineGLCB(t *testing.T, ctx context.Context, n int, raw string, attrs record.Attributes) sealedGLCBFixture {
	t.Helper()
	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAttributed(t, ctx, n, raw, attrs)

	home := t.TempDir()
	copyCompletedToHead(t, origin.root, home, segID)

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunkingSpec(home, fsm, func() bool { return true })); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	planUntilOpenRef(t, ctx, mgr, fsm, vaultID)
	if err := mgr.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("RotateCron: %v", err)
	}
	sealedManifest := fsm.SealedManifest()
	if sealedManifest == nil {
		t.Fatal("manifest not sealed")
	}
	chunkID := sealedManifest.ChunkID
	if err := mgr.BuildOnce(ctx, vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		t.Fatalf("GLCB not built: %v", err)
	}

	var sealed vaultctlfsm.ManifestEntry
	for _, e := range fsm.List() {
		if e.ID == chunkID && e.IsSealed() {
			sealed = e
			break
		}
	}
	if sealed.ID != chunkID {
		t.Fatalf("sealed entry for %s not found in FSM", chunkID)
	}
	if sealed.RecordCount == 0 {
		t.Fatal("sealed entry has zero records")
	}
	return sealedGLCBFixture{vaultID: vaultID, fsm: fsm, sealed: sealed, glcbPath: glcbPath}
}

// externalInfoFromEntry mirrors VaultLifecycleReconciler.registerPipelineGLCB.
func externalInfoFromEntry(e vaultctlfsm.ManifestEntry) chunk.ExternalGLCBInfo {
	return chunk.ExternalGLCBInfo{
		WriteStart:        e.WriteStart,
		WriteEnd:          e.WriteEnd,
		IngestStart:       e.IngestStart,
		IngestEnd:         e.IngestEnd,
		SourceStart:       e.SourceStart,
		SourceEnd:         e.SourceEnd,
		RecordCount:       e.RecordCount,
		Bytes:             e.Bytes,
		DiskBytes:         e.DiskBytes,
		IngestIdxOffset:   e.IngestIdxOffset,
		IngestIdxSize:     e.IngestIdxSize,
		SourceIdxOffset:   e.SourceIdxOffset,
		SourceIdxSize:     e.SourceIdxSize,
		IngestTSMonotonic: e.IngestTSMonotonic,
	}
}

// newQueryCM builds a file chunk manager rooted at a fresh dir distinct from the
// pipeline ChunkRoot (the legacy vault chunk-manager dir), plus a file index
// manager over the same dir (no sidecars → sequential scan).
func newQueryCM(t *testing.T) (*chunkfile.Manager, index.IndexManager) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatalf("new chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	return cm, indexfile.NewManager(dir, nil, nil, cm)
}

// drainSearch runs Search and collects the records (failing on any error).
func drainSearch(t *testing.T, eng *query.Engine, q query.Query) []chunk.Record {
	t.Helper()
	it, _ := eng.Search(context.Background(), q, nil)
	var out []chunk.Record
	for rec, err := range it {
		if err != nil {
			t.Fatalf("search error: %v", err)
		}
		out = append(out, rec)
	}
	return out
}

// TestPipelineSealedGLCBQueryableSingleNode: a pipeline-built sealed GLCB
// becomes queryable on a holder node only after RegisterExternalGLCB; the
// records come back through query.Engine.Search by time range, and a range that
// excludes them returns nothing.
func TestPipelineSealedGLCBQueryableSingleNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api"})
	cm, im := newQueryCM(t)

	// Before registration: byte access is the gap — OpenCursor cannot find it.
	if _, err := cm.OpenCursor(fx.sealed.ID); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("OpenCursor before registration = %v, want ErrChunkNotFound", err)
	}

	registrar, ok := any(cm).(chunk.ExternalGLCBRegistrar)
	if !ok {
		t.Fatal("file chunk manager does not implement ExternalGLCBRegistrar")
	}
	if err := registrar.RegisterExternalGLCB(fx.sealed.ID, fx.glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	eng := query.New(cm, im, nil)

	// No time bound: every record in the chunk comes back.
	all := drainSearch(t, eng, query.Query{})
	if int64(len(all)) != fx.sealed.RecordCount {
		t.Fatalf("unbounded search returned %d records, want %d (sealed RecordCount)", len(all), fx.sealed.RecordCount)
	}
	for _, rec := range all {
		if string(rec.Raw) != e1Payload {
			t.Fatalf("record Raw = %q, want %q", rec.Raw, e1Payload)
		}
	}

	// A wide range that brackets 2025 returns everything.
	wide := drainSearch(t, eng, query.Query{
		Start: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if len(wide) != len(all) {
		t.Fatalf("wide range returned %d, want %d", len(wide), len(all))
	}

	// A range entirely after the records excludes them all.
	none := drainSearch(t, eng, query.Query{
		Start: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if len(none) != 0 {
		t.Fatalf("future range returned %d records, want 0", len(none))
	}
}

// TestPipelineSealedGLCBEmptyAttributesQueryable: records ingested without
// attributes round-trip through build → register → query. Guards the Rubicon D
// empty-attribute decode fix at the query layer.
func TestPipelineSealedGLCBEmptyAttributesQueryable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, nil)
	cm, im := newQueryCM(t)
	if err := cm.RegisterExternalGLCB(fx.sealed.ID, fx.glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	eng := query.New(cm, im, nil)
	all := drainSearch(t, eng, query.Query{})
	if int64(len(all)) != fx.sealed.RecordCount {
		t.Fatalf("empty-attr search returned %d records, want %d", len(all), fx.sealed.RecordCount)
	}
	for _, rec := range all {
		if string(rec.Raw) != e1Payload {
			t.Fatalf("record Raw = %q, want %q", rec.Raw, e1Payload)
		}
		if len(rec.Attrs) != 0 {
			t.Fatalf("expected empty attrs, got %v", rec.Attrs)
		}
	}
}

// TestPipelineSealedGLCBBoolFilterSequential: with no index sidecars, bool
// filters (token + attribute key=value) are answered by the sequential scan +
// runtime filter path. Matching filters return the records; non-matching
// filters return none — and neither errors.
func TestPipelineSealedGLCBBoolFilterSequential(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api"})
	cm, im := newQueryCM(t)
	if err := cm.RegisterExternalGLCB(fx.sealed.ID, fx.glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	eng := query.New(cm, im, nil)

	total := int(fx.sealed.RecordCount)

	// Token present in Raw (hyphens are token bytes → whole string is one token).
	tokHit := drainSearch(t, eng, query.Query{Tokens: []string{e1Payload}})
	if len(tokHit) != total {
		t.Fatalf("matching token filter returned %d, want %d", len(tokHit), total)
	}
	// Token absent → zero matches, no error.
	tokMiss := drainSearch(t, eng, query.Query{Tokens: []string{"zzz-absent-token"}})
	if len(tokMiss) != 0 {
		t.Fatalf("non-matching token filter returned %d, want 0", len(tokMiss))
	}

	// Attribute key=value present on every record.
	kvHit := drainSearch(t, eng, query.Query{KV: []query.KeyValueFilter{{Key: "service", Value: "api"}}})
	if len(kvHit) != total {
		t.Fatalf("matching attr filter returned %d, want %d", len(kvHit), total)
	}
	// Attribute value mismatch → zero matches.
	kvMiss := drainSearch(t, eng, query.Query{KV: []query.KeyValueFilter{{Key: "service", Value: "db"}}})
	if len(kvMiss) != 0 {
		t.Fatalf("non-matching attr filter returned %d, want 0", len(kvMiss))
	}
}

// e1Registry is a minimal multi-vault registry whose Reader returns a fixed set
// of sealed entries (modeling the vault-ctl FSM truth) independent of the chunk
// manager's own state. This lets us model a node that knows about a chunk via
// the FSM but does not hold its bytes.
type e1Registry struct {
	vaultID glid.GLID
	cm      chunk.ChunkManager
	im      index.IndexManager
	entries []vaultctlfsm.ManifestEntry
}

func (r *e1Registry) ListVaults() []glid.GLID { return []glid.GLID{r.vaultID} }

func (r *e1Registry) ChunkManager(id glid.GLID) chunk.ChunkManager {
	if id == r.vaultID {
		return r.cm
	}
	return nil
}

func (r *e1Registry) IndexManager(id glid.GLID) index.IndexManager {
	if id == r.vaultID {
		return r.im
	}
	return nil
}

func (r *e1Registry) Reader() manifest.Reader           { return &e1Reader{r: r} }
func (r *e1Registry) IndexReader() manifest.IndexReader { return e1NoIndexReader{} }

type e1Reader struct{ r *e1Registry }

func (rd *e1Reader) Entry(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
	for _, e := range rd.r.entries {
		if e.ID == id {
			return e, true
		}
	}
	return vaultctlfsm.ManifestEntry{}, false
}

func (rd *e1Reader) EntriesForVault(id glid.GLID) []vaultctlfsm.ManifestEntry {
	if id == rd.r.vaultID {
		return rd.r.entries
	}
	return nil
}

type e1NoIndexReader struct{}

func (e1NoIndexReader) FindIngestRank(chunk.ChunkID, time.Time) (uint64, bool) { return 0, false }
func (e1NoIndexReader) FindIngestPos(chunk.ChunkID, time.Time) (uint64, bool)  { return 0, false }

// TestPipelineSealedGLCBMultiNode: in registry mode (chunks discovered from the
// FSM-shaped entries), a holder node that registered the external GLCB serves
// the query, while a node that has the same FSM entry but not the bytes skips
// the chunk gracefully — zero records, no error.
func TestPipelineSealedGLCBMultiNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api"})
	entries := []vaultctlfsm.ManifestEntry{fx.sealed}

	// Holder: registers the external GLCB it built → serves the query.
	holderCM, holderIM := newQueryCM(t)
	if err := holderCM.RegisterExternalGLCB(fx.sealed.ID, fx.glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
		t.Fatalf("holder RegisterExternalGLCB: %v", err)
	}
	holderEng := query.NewWithRegistry(&e1Registry{vaultID: fx.vaultID, cm: holderCM, im: holderIM, entries: entries}, nil)
	served := drainSearch(t, holderEng, query.Query{})
	if int64(len(served)) != fx.sealed.RecordCount {
		t.Fatalf("holder served %d records, want %d", len(served), fx.sealed.RecordCount)
	}

	// Missing-bytes node: same FSM entry, but never registered the GLCB →
	// OpenCursor returns ErrChunkNotFound, which the engine skips silently.
	missCM, missIM := newQueryCM(t)
	missEng := query.NewWithRegistry(&e1Registry{vaultID: fx.vaultID, cm: missCM, im: missIM, entries: entries}, nil)
	skipped := drainSearch(t, missEng, query.Query{})
	if len(skipped) != 0 {
		t.Fatalf("missing-bytes node returned %d records, want 0 (graceful skip)", len(skipped))
	}
}

func TestExternalGLCBInfoForPipelineReadsIndexFromFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fx := buildSealedPipelineGLCB(t, ctx, 6, e1Payload, nil)
	sealingEntry := vaultctlfsm.ManifestEntry{ID: fx.sealed.ID, State: chunk.ChunkStateSealing}
	info, err := externalGLCBInfoForPipeline(sealingEntry, fx.glcbPath)
	if err != nil {
		t.Fatalf("externalGLCBInfoForPipeline: %v", err)
	}
	if info.RecordCount != fx.sealed.RecordCount {
		t.Fatalf("RecordCount = %d, want %d", info.RecordCount, fx.sealed.RecordCount)
	}
	if info.IngestIdxOffset == 0 || info.IngestIdxSize == 0 {
		t.Fatalf("ingest index not populated from GLCB footer")
	}
	if info.IngestStart.IsZero() || info.IngestEnd.IsZero() {
		t.Fatalf("ingest bounds not populated from GLCB")
	}
}
