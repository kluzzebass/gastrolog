package pipeline_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

// emitIngester pushes a fixed batch then blocks until cancelled.
type emitIngester struct {
	msgs []ingestion.IngesterMessage
}

func (e *emitIngester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	for _, msg := range e.msgs {
		select {
		case out <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// harness wires ingestion → digestion → routing → segmentation the way the
// orchestrator will (gastrolog-214bz), without control-plane or ack semantics yet.
type harness struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	nodeID     glid.GLID
	ingesterID glid.GLID
	vaultID    glid.GLID
	vaultRoot  string

	ingest *ingestion.Manager
	route  *routing.Manager
	digest *digestion.Manager
	seg    *segmentation.Manager

	syncs atomic.Uint32
}

func newHarness(t *testing.T, nodeID, ingesterID, vaultID glid.GLID, route *routing.Route) *harness {
	t.Helper()

	vaultRoot := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())

	h := &harness{
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
		nodeID:     nodeID,
		ingesterID: ingesterID,
		vaultID:    vaultID,
		vaultRoot:  vaultRoot,
	}

	segMgr, _ := segmentation.New(segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Millisecond,
		OnSync:          func() { h.syncs.Add(1) },
	})
	vaultIn, err := segMgr.RegisterVault(vaultID, vaultRoot)
	if err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	routeMgr := routing.New(routing.Config{
		Workers: 2,
		Table:   routing.NewTable([]*routing.Route{route}),
		Vaults: map[glid.GLID]chan<- *record.Record{
			vaultID: vaultIn,
		},
	})

	digestMgr, digestOut := digestion.New(digestion.Config{
		Workers:     2,
		OutCapacity: 16,
	})

	routingIn := make(chan routing.Input, 16)

	ingestMgr, ingestOut := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 16,
	})

	h.ingest = ingestMgr
	h.route = routeMgr
	h.digest = digestMgr
	h.seg = segMgr

	go func() {
		_ = h.seg.Run(ctx)
		close(h.done)
	}()

	go func() {
		defer close(routingIn)
		for out := range digestOut {
			if out.Err != nil {
				if out.Ack != nil {
					out.Ack <- out.Err
				}
				continue
			}
			select {
			case routingIn <- routing.IngestInput(out.Record):
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() { _ = h.route.Run(ctx, routingIn) }()
	go func() { _ = h.digest.Run(ctx, ingestOut) }()

	t.Cleanup(func() {
		cancel()
		<-h.done
	})

	return h
}

func (h *harness) runIngester(t *testing.T, ing ingestion.Ingester) {
	t.Helper()
	if err := h.ingest.Reconcile([]ingestion.IngesterSpec{
		{ID: h.ingesterID, Ingester: ing, Name: "test", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := h.ingest.Start(h.ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = h.ingest.Stop()
	})
}

func (h *harness) waitSyncs(t *testing.T, want uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.syncs.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("fsync count = %d, want >= %d", h.syncs.Load(), want)
}

func (h *harness) readWorkingRecords(t *testing.T) []record.Record {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(h.vaultRoot, "working"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments = %d, want 1", len(entries))
	}
	path := filepath.Join(h.vaultRoot, "working", entries[0].Name())
	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	got, err := sf.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestPipelineIngestToSegment(t *testing.T) {
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()
	sourceTS := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route)
	h.runIngester(t, &emitIngester{msgs: []ingestion.IngesterMessage{
		{Raw: []byte("first line"), Attrs: map[string]string{"env": "prod"}, SourceTS: sourceTS},
		{Raw: []byte("second line"), Attrs: map[string]string{"env": "prod"}},
	}})

	h.waitSyncs(t, 2)

	stats := h.route.Stats()
	if stats.Matched != 2 || stats.Unmatched != 0 {
		t.Fatalf("routing stats = %+v, want matched=2 unmatched=0", stats)
	}

	got := h.readWorkingRecords(t)
	if len(got) != 2 {
		t.Fatalf("segment records = %d, want 2", len(got))
	}
	byRaw := map[string]record.Record{
		string(got[0].Raw): got[0],
		string(got[1].Raw): got[1],
	}
	first, ok := byRaw["first line"]
	if !ok {
		t.Fatalf("missing first line in segment: %+v", got)
	}
	if _, ok := byRaw["second line"]; !ok {
		t.Fatalf("missing second line in segment: %+v", got)
	}
	if first.Attrs["env"] != "prod" {
		t.Fatalf("attrs = %v", first.Attrs)
	}
	if first.EventID.IngesterID != ingesterID || first.EventID.NodeID != nodeID {
		t.Fatalf("EventID = %+v, want ingester=%s node=%s", first.EventID, ingesterID, nodeID)
	}
	if !first.SourceTS.Equal(sourceTS) {
		t.Fatalf("SourceTS = %v, want %v", first.SourceTS, sourceTS)
	}
	if first.WriteTS.IsZero() {
		t.Error("WriteTS should be set at segment append")
	}
	if first.EventID.IngestSeq >= byRaw["second line"].EventID.IngestSeq {
		t.Fatalf("IngestSeq order = %d, %d; mint order should be preserved on EventID",
			first.EventID.IngestSeq, byRaw["second line"].EventID.IngestSeq)
	}
}

func TestPipelineFanOutTwoVaults(t *testing.T) {
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultA := glid.New()
	vaultB := glid.New()

	route, err := routing.CompileRoute(glid.New(), "fan", 0, "*", []glid.GLID{vaultA, vaultB})
	if err != nil {
		t.Fatal(err)
	}

	rootA := t.TempDir()
	rootB := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	var syncs atomic.Uint32
	segMgr, _ := segmentation.New(segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Millisecond,
		OnSync:          func() { syncs.Add(1) },
	})
	inA, err := segMgr.RegisterVault(vaultA, rootA)
	if err != nil {
		t.Fatal(err)
	}
	inB, err := segMgr.RegisterVault(vaultB, rootB)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		_ = segMgr.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	routeMgr := routing.New(routing.Config{
		Table: routing.NewTable([]*routing.Route{route}),
		Vaults: map[glid.GLID]chan<- *record.Record{
			vaultA: inA,
			vaultB: inB,
		},
	})
	digestMgr, digestOut := digestion.New(digestion.Config{Workers: 1, OutCapacity: 4})
	routingIn := make(chan routing.Input, 4)
	ingestMgr, ingestOut := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 4})

	go func() {
		defer close(routingIn)
		for out := range digestOut {
			if out.Err != nil {
				continue
			}
			routingIn <- routing.IngestInput(out.Record)
		}
	}()
	go func() { _ = routeMgr.Run(ctx, routingIn) }()
	go func() { _ = digestMgr.Run(ctx, ingestOut) }()

	if err := ingestMgr.Reconcile([]ingestion.IngesterSpec{
		{ID: ingesterID, Ingester: &emitIngester{msgs: []ingestion.IngesterMessage{
			{Raw: []byte("shared"), Attrs: map[string]string{"k": "v"}},
		}}, Name: "test", Type: "mock"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ingestMgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ingestMgr.Stop() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && syncs.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	if syncs.Load() < 2 {
		t.Fatalf("fsync count = %d, want 2 (one per vault)", syncs.Load())
	}

	for _, root := range []string{rootA, rootB} {
		entries, err := os.ReadDir(filepath.Join(root, "working"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("%s: working segments = %d", root, len(entries))
		}
		path := filepath.Join(root, "working", entries[0].Name())
		sf, err := segment.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		recs, err := sf.ReadAll()
		_ = sf.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 1 || string(recs[0].Raw) != "shared" {
			t.Fatalf("%s: records = %+v", root, recs)
		}
	}
}

func TestPipelineUnmatchedNotWritten(t *testing.T) {
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "prod", 0, "env=prod", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route)
	h.runIngester(t, &emitIngester{msgs: []ingestion.IngesterMessage{
		{Raw: []byte("staging"), Attrs: map[string]string{"env": "staging"}},
	}})

	time.Sleep(100 * time.Millisecond)

	stats := h.route.Stats()
	if stats.Matched != 0 || stats.Unmatched != 1 {
		t.Fatalf("routing stats = %+v, want matched=0 unmatched=1", stats)
	}
	if h.syncs.Load() != 0 {
		t.Fatalf("unexpected fsync count = %d", h.syncs.Load())
	}

	entries, err := os.ReadDir(filepath.Join(h.vaultRoot, "working"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments = %d", len(entries))
	}
	path := filepath.Join(h.vaultRoot, "working", entries[0].Name())
	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	if sf.Header().RecordCount != 0 {
		t.Fatalf("segment record count = %d, want 0", sf.Header().RecordCount)
	}
}
