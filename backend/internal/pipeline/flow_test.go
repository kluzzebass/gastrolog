package pipeline_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
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

// harness wires ingestion → digestion → routing → segmentation → distribution
// (→ collection when opts.withCollection), the way the orchestrator will
// (gastrolog-214bz), without control-plane or ack semantics yet.
type harness struct {
	ctx    context.Context
	cancel context.CancelFunc
	segDone   chan struct{}
	distDone  chan struct{}
	colDone    chan struct{}
	chunkDone  chan struct{}

	nodeID     glid.GLID
	ingesterID glid.GLID
	vaultID    glid.GLID
	vaultRoot  string
	homeRoot   string

	ingest   *ingestion.Manager
	route    *routing.Manager
	digest   *digestion.Manager
	seg      *segmentation.Manager
	dist     *distribution.Manager
	collect   *collection.Manager
	chunk     *chunking.Manager
	pub       *recordingPublisher
	receipts  *flowCollectionReceipts
	fsm       *vaultctlfsm.FSM
	chunkID   chunk.ChunkID
	chunkRoot string
	gatePull  *gateFlowPull

	syncs atomic.Uint32
}

type recordingPublisher struct {
	mu        sync.Mutex
	published []distribution.Metadata
	notify    func()
}

func (p *recordingPublisher) Publish(_ context.Context, meta distribution.Metadata) error {
	p.mu.Lock()
	p.published = append(p.published, meta)
	notify := p.notify
	p.mu.Unlock()
	if notify != nil {
		notify()
	}
	return nil
}

func (p *recordingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

func (p *recordingPublisher) first() distribution.Metadata {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.published) == 0 {
		return distribution.Metadata{}
	}
	return p.published[0]
}

func (p *recordingPublisher) all() []distribution.Metadata {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]distribution.Metadata, len(p.published))
	copy(out, p.published)
	return out
}

type harnessOpts struct {
	closePolicy    segmentation.ClosePolicy
	localHolder    bool
	withCollection bool // remote home: collection on a separate storage root
	withChunking   bool // ChunkingManager on homeRoot via shared vault-ctl FSM
	chunkPolicy    chunking.ManifestRotationPolicy
	chunkLeader    func() bool // nil defaults to vault leader
	pullFails      int         // simulate transient origin pull failures on remote home
	gatedPull      bool        // block pulls until allowPull() (multi-node recovery tests)
}

type gateFlowPull struct {
	inner *flowDistributionPull
	open  atomic.Bool
}

func (p *gateFlowPull) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	if !p.open.Load() {
		return fmt.Errorf("origin unreachable")
	}
	return p.inner.Pull(ctx, vaultID, segmentID, dest)
}

func (p *gateFlowPull) allow() {
	p.open.Store(true)
}

type flowFsmApplier struct {
	fsm *vaultctlfsm.FSM
}

func (a *flowFsmApplier) Apply(data []byte) error {
	cp := append([]byte(nil), data...)
	if result := a.fsm.Apply(&hraft.Log{Data: cp}); result != nil {
		if err, ok := result.(error); ok {
			return fmt.Errorf("apply: %w", err)
		}
		return fmt.Errorf("apply: %v", result)
	}
	return nil
}

// flowVaultCtlPublisher records metadata for collection and commits segment
// registry entries to the harness vault-ctl FSM.
type flowVaultCtlPublisher struct {
	rec  *recordingPublisher
	vctl *distribution.VaultCtlPublisher
}

func (p *flowVaultCtlPublisher) Publish(ctx context.Context, meta distribution.Metadata) error {
	if err := p.rec.Publish(ctx, meta); err != nil {
		return err
	}
	return p.vctl.Publish(ctx, meta)
}

type flowCollectionNudger struct {
	collect *collection.Manager
	vaultID glid.GLID
}

func (n flowCollectionNudger) CollectMissing(ctx context.Context) error {
	return n.collect.CollectOnce(ctx, n.vaultID)
}

type flakyFlowPull struct {
	inner    *flowDistributionPull
	attempts atomic.Int32
	maxFail  int32
}

func (p *flakyFlowPull) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	if p.attempts.Add(1) <= p.maxFail {
		return fmt.Errorf("origin node unreachable")
	}
	return p.inner.Pull(ctx, vaultID, segmentID, dest)
}

func applyFlowFSM(t *testing.T, fsm *vaultctlfsm.FSM, data []byte) {
	t.Helper()
	if result := fsm.Apply(&hraft.Log{Data: data}); result != nil {
		if err, ok := result.(error); ok {
			t.Fatalf("apply: %v", err)
		}
		t.Fatalf("apply: %v", result)
	}
}

func newHarness(t *testing.T, nodeID, ingesterID, vaultID glid.GLID, route *routing.Route, opts harnessOpts) *harness {
	t.Helper()

	if opts.withChunking && !opts.withCollection {
		t.Fatal("withChunking requires withCollection")
	}

	vaultRoot := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())

	h := &harness{
		ctx:        ctx,
		cancel:     cancel,
		segDone:    make(chan struct{}),
		distDone:   make(chan struct{}),
		nodeID:     nodeID,
		ingesterID: ingesterID,
		vaultID:    vaultID,
		vaultRoot:  vaultRoot,
		pub:        &recordingPublisher{},
	}

	if opts.withCollection {
		if opts.localHolder {
			t.Fatal("withCollection and localHolder are mutually exclusive in flow tests")
		}
		h.homeRoot = t.TempDir()
		h.receipts = &flowCollectionReceipts{}
		h.colDone = make(chan struct{})
	}

	var distPublisher distribution.Publisher = h.pub
	if opts.withChunking {
		h.fsm = vaultctlfsm.New()
		h.chunkID = chunk.NewChunkID()
		distPublisher = &flowVaultCtlPublisher{
			rec: h.pub,
			vctl: &distribution.VaultCtlPublisher{
				Applier:      &flowFsmApplier{fsm: h.fsm},
				OriginNodeID: nodeID.String(),
			},
		}
	}

	segMgr, completed := segmentation.New(segmentation.Config{
		ClosePolicy:     opts.closePolicy,
		SyncBatchSize:   1,
		SyncBatchWindow: time.Millisecond,
		OnSync:          func() { h.syncs.Add(1) },
	})
	vaultIn, err := segMgr.RegisterVault(vaultID, vaultRoot, segmentation.VaultConfig{})
	if err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	distMgr, _ := distribution.New(distribution.Config{})
	if err := distMgr.RegisterVault(vaultID, vaultRoot, distribution.VaultConfig{
		Publisher:   distPublisher,
		LocalHolder: func() bool { return opts.localHolder },
	}); err != nil {
		t.Fatalf("RegisterVault distribution: %v", err)
	}

	routeMgr := routing.New(routing.Config{
		Workers: 2,
		Table:   routing.NewTable([]*routing.Route{route}),
		Vaults: map[glid.GLID]chan<- segmentation.Input{
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
	h.dist = distMgr

	go func() {
		_ = h.seg.Run(ctx)
		close(h.segDone)
	}()
	go func() {
		_ = h.dist.Run(ctx, completed)
		close(h.distDone)
	}()

	if opts.withCollection {
		colMgr := collection.New(collection.Config{})
		basePull := &flowDistributionPull{dist: distMgr}
		var pull collection.PullClient = basePull
		if opts.gatedPull {
			h.gatePull = &gateFlowPull{inner: basePull}
			pull = h.gatePull
		} else if opts.pullFails > 0 {
			pull = &flakyFlowPull{
				inner:   basePull,
				maxFail: int32(opts.pullFails),
			}
		}
		if err := colMgr.RegisterVault(vaultID, h.homeRoot, collection.VaultConfig{
			Log:      &publishedLog{pub: h.pub, vaultID: vaultID},
			Pull:     pull,
			Receipts: h.receipts,
		}); err != nil {
			t.Fatalf("RegisterVault collection: %v", err)
		}
		h.collect = colMgr
		h.pub.notify = func() { colMgr.Notify(vaultID) }
		go func() {
			_ = h.collect.Run(ctx)
			close(h.colDone)
		}()
	}

	if opts.withChunking {
		policy := opts.chunkPolicy
		if policy.MaxRecords == 0 && policy.MaxBytes == 0 && policy.MaxAge == 0 {
			policy = chunking.ManifestRotationPolicy{MaxRecords: 100}
		}
		h.chunkRoot = filepath.Join(h.homeRoot, "chunks")
		chunkMgr := chunking.New(chunking.Config{})
		chunkID := h.chunkID
		isLeader := func() bool { return true }
		if opts.chunkLeader != nil {
			isLeader = opts.chunkLeader
		}
		if err := chunkMgr.RegisterVault(vaultID, chunking.VaultConfig{
			VaultRoot:  h.homeRoot,
			ChunkRoot:  h.chunkRoot,
			FSM:        h.fsm,
			Locate:     chunking.VaultSegmentLocator{Root: h.homeRoot},
			Applier:    &flowFsmApplier{fsm: h.fsm},
			IsLeader:   isLeader,
			NewChunkID: func() chunk.ChunkID { return chunkID },
			Policy:     policy,
			Nudge:      flowCollectionNudger{collect: h.collect, vaultID: vaultID},
		}); err != nil {
			t.Fatalf("RegisterVault chunking: %v", err)
		}
		h.chunk = chunkMgr
		h.chunkDone = make(chan struct{})
		go func() {
			_ = h.chunk.Run(ctx)
			close(h.chunkDone)
		}()
	}

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
		<-h.segDone
		<-h.distDone
		if h.colDone != nil {
			<-h.colDone
		}
		if h.chunkDone != nil {
			<-h.chunkDone
		}
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

func (h *harness) waitPublished(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.pub.count() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("published %d segments, want >= %d", h.pub.count(), want)
}

func (h *harness) waitPublishedStable(t *testing.T, min int) []distribution.Metadata {
	t.Helper()
	h.waitPublished(t, min)
	deadline := time.Now().Add(2 * time.Second)
	var last int
	for time.Now().Before(deadline) {
		n := h.pub.count()
		if n >= min && n == last {
			return h.pub.all()
		}
		last = n
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("published count did not stabilize at >= %d (last %d)", min, last)
	return nil
}

func (h *harness) waitCollected(t *testing.T, want int) {
	t.Helper()
	if h.receipts == nil {
		t.Fatal("harness has no collection")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n := h.receipts.receiptCount()
		if n == want {
			return
		}
		if n > want {
			t.Fatalf("collected %d segments, want %d", n, want)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("collected %d segments, want %d", h.receipts.receiptCount(), want)
}

func (h *harness) waitCollectedWithRetry(t *testing.T, want int) {
	t.Helper()
	if h.collect == nil {
		t.Fatal("harness has no collection")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n := h.receipts.receiptCount()
		if n == want {
			return
		}
		if n > want {
			t.Fatalf("collected %d segments, want %d", n, want)
		}
		h.collect.Notify(h.vaultID)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("collected %d segments, want %d", h.receipts.receiptCount(), want)
}

func (h *harness) waitChunkGLCB(t *testing.T, wantRecords uint32) string {
	t.Helper()
	if h.chunk == nil {
		t.Fatal("harness has no chunking")
	}
	glcbPath := chunking.ChunkGLCBPath(h.chunkRoot, h.chunkID)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(glcbPath); err == nil {
			f, err := os.Open(glcbPath)
			if err != nil {
				t.Fatal(err)
			}
			rd, err := chunkcloud.NewCacheReader(f)
			_ = f.Close()
			if err != nil {
				t.Fatal(err)
			}
			if rd.Meta().RecordCount != wantRecords {
				rd.Close()
				time.Sleep(10 * time.Millisecond)
				continue
			}
			rd.Close()
			return glcbPath
		}
		if err := h.chunk.PlanOnce(h.ctx, h.vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
		if h.fsm.SealedManifest() != nil {
			if err := h.chunk.BuildOnce(h.ctx, h.vaultID); err != nil {
				t.Fatalf("BuildOnce: %v", err)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GLCB not built at %s", glcbPath)
	return ""
}

func (h *harness) readCompletedRecords(t *testing.T) []record.Record {
	t.Helper()
	entries, err := os.ReadDir(paths.CompletedDir(h.vaultRoot))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("completed segments = %d, want 1", len(entries))
	}
	path := filepath.Join(paths.CompletedDir(h.vaultRoot), entries[0].Name())
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

func (h *harness) waitCompletedEmpty(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(paths.CompletedDir(h.vaultRoot))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("completed dir not empty after local promote")
}

func (h *harness) readWorkingRecords(t *testing.T) []record.Record {
	t.Helper()
	entries, err := os.ReadDir(paths.WorkingDir(h.vaultRoot))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments = %d, want 1", len(entries))
	}
	path := filepath.Join(paths.WorkingDir(h.vaultRoot), entries[0].Name())
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

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{})
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

func TestPipelineIngestToDistribution(t *testing.T) {
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{
		closePolicy: segmentation.ClosePolicy{MaxBytes: 256},
	})
	msgs := make([]ingestion.IngesterMessage, 8)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{
			Raw:   []byte("line payload for segment close"),
			Attrs: map[string]string{"env": "prod"},
		}
	}
	h.runIngester(t, &emitIngester{msgs: msgs})

	h.waitSyncs(t, 8)
	h.waitPublished(t, 1)

	meta := h.pub.first()
	if meta.RecordCount == 0 {
		t.Fatalf("published metadata = %+v", meta)
	}

	path := paths.CompletedSegment(h.vaultRoot, meta.SegmentID)
	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sf.ReadAll()
	_ = sf.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("completed segment has no records")
	}

	var buf bytes.Buffer
	if err := h.dist.ServePull(distribution.PullRequest{
		VaultID:   vaultID,
		SegmentID: meta.SegmentID,
		Dest:      &buf,
	}); err != nil {
		t.Fatalf("ServePull: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("line payload")) {
		t.Fatalf("pull payload = %q", buf.Bytes())
	}
}

func TestPipelineIngestToDistributionLocalHolder(t *testing.T) {
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{
		closePolicy: segmentation.ClosePolicy{MaxBytes: 256},
		localHolder: true,
	})
	msgs := make([]ingestion.IngesterMessage, 8)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{
			Raw:   []byte("local holder payload"),
			Attrs: map[string]string{"env": "prod"},
		}
	}
	h.runIngester(t, &emitIngester{msgs: msgs})

	h.waitSyncs(t, 8)
	h.waitCompletedEmpty(t)
	if h.pub.count() < 1 {
		t.Fatalf("published %d segments", h.pub.count())
	}

	headEntries, err := os.ReadDir(paths.HeadDir(h.vaultRoot))
	if err != nil {
		t.Fatal(err)
	}
	if len(headEntries) < 1 {
		t.Fatalf("head segments = %d, want >= 1", len(headEntries))
	}
}

func TestPipelineFullPath(t *testing.T) {
	// Ingestion → digestion → routing → segmentation → distribution →
	// collection (remote home) → ChunkingManager planner + build-at-seal.
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{
		closePolicy:    segmentation.ClosePolicy{MaxBytes: 256},
		withCollection: true,
		withChunking:   true,
		chunkPolicy:    chunking.ManifestRotationPolicy{MaxRecords: 8},
	})
	msgs := make([]ingestion.IngesterMessage, 8)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{
			Raw:   []byte("full path payload"),
			Attrs: map[string]string{"env": "prod"},
		}
	}
	h.runIngester(t, &emitIngester{msgs: msgs})

	h.waitSyncs(t, 8)
	published := h.waitPublishedStable(t, 1)
	h.waitCollected(t, len(published))

	meta := published[0]
	if meta.RecordCount == 0 {
		t.Fatalf("published metadata = %+v", meta)
	}

	originPath := paths.CompletedSegment(h.vaultRoot, meta.SegmentID)
	if _, err := os.Stat(originPath); err != nil {
		t.Fatalf("origin completed segment: %v", err)
	}

	headPath := paths.HeadSegment(h.homeRoot, meta.SegmentID)
	sf, err := segment.Open(headPath)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sf.ReadAll()
	_ = sf.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("collected head segment has no records")
	}
	if !bytes.Contains(got[0].Raw, []byte("full path payload")) {
		t.Fatalf("payload = %q", got[0].Raw)
	}
	if h.receipts.receiptCount() != len(published) {
		t.Fatalf("receipts = %d, want %d (published segments)", h.receipts.receiptCount(), len(published))
	}

	if len(h.fsm.ListCompletedSegments()) != len(published) {
		t.Fatalf("FSM completed segments = %d, want %d", len(h.fsm.ListCompletedSegments()), len(published))
	}

	var totalRecords uint32
	for _, meta := range published {
		totalRecords += meta.RecordCount
	}

	glcbPath := h.waitChunkGLCB(t, totalRecords)
	f, err := os.Open(glcbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rd, err := chunkcloud.NewCacheReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	if rd.Meta().RecordCount != totalRecords {
		t.Fatalf("GLCB record count = %d, want %d", rd.Meta().RecordCount, totalRecords)
	}
	var prev chunk.EventID
	for i := range totalRecords {
		rec, err := rd.ReadRecord(i)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if i > 0 && prev.Compare(rec.EventID) >= 0 {
			t.Fatalf("GLCB record %d not sorted by EventID", i)
		}
		prev = rec.EventID
		if !bytes.Contains(rec.Raw, []byte("full path payload")) {
			t.Fatalf("GLCB payload = %q", rec.Raw)
		}
	}
	if h.fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest should clear after SealChunk")
	}
	entry := h.fsm.Get(h.chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want sealed", entry)
	}
}

func TestPipelineOpenChunkQueryBeforeSeal(t *testing.T) {
	// Virtual open-chunk query serves manifest-listed refs before GLCB exists.
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{
		closePolicy:    segmentation.ClosePolicy{MaxBytes: 256},
		withCollection: true,
		withChunking:   true,
		chunkPolicy:    chunking.ManifestRotationPolicy{MaxRecords: 100},
	})
	msgs := make([]ingestion.IngesterMessage, 8)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{
			Raw:   []byte("open chunk query payload"),
			Attrs: map[string]string{"env": "prod"},
		}
	}
	h.runIngester(t, &emitIngester{msgs: msgs})

	h.waitSyncs(t, 8)
	published := h.waitPublishedStable(t, 1)
	h.waitCollected(t, len(published))

	var totalRecords uint32
	for _, meta := range published {
		totalRecords += meta.RecordCount
	}

	// Ref-adds are asynchronous (worker goroutine chained off FSM callbacks),
	// so poll until the open manifest covers every published record. PlanOnce
	// nudges the chain along in case a wake was consumed before collection
	// finished.
	var open *vaultctlfsm.OpenChunkManifest
	planDeadline := time.Now().Add(10 * time.Second)
	for {
		open = h.fsm.OpenChunk()
		if open != nil && open.TotalRecords == uint64(totalRecords) {
			break
		}
		if time.Now().After(planDeadline) {
			t.Fatalf("open manifest never covered %d records (open=%+v)", totalRecords, open)
		}
		if err := h.chunk.PlanOnce(h.ctx, h.vaultID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h.fsm.SealedManifest() != nil {
		t.Fatal("chunk must still be open")
	}

	got, report, err := chunking.CollectOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: open,
		Locate:   chunking.VaultSegmentLocator{Root: h.homeRoot},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.MissingSegments) != 0 {
		t.Fatalf("missing segments on home: %v", report.MissingSegments)
	}
	if uint32(len(got)) != totalRecords {
		t.Fatalf("open query records = %d, want %d", len(got), totalRecords)
	}
	for _, rec := range got {
		if !bytes.Contains(rec.Raw, []byte("open chunk query payload")) {
			t.Fatalf("payload = %q", rec.Raw)
		}
	}
	if _, err := os.Stat(chunking.ChunkGLCBPath(h.chunkRoot, h.chunkID)); err == nil {
		t.Fatal("GLCB must not exist while chunk is open")
	}
}

func TestPipelineRemotePullFailureThenRecovery(t *testing.T) {
	// Origin holds completed segments; remote home retries collection after pull failure.
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{
		closePolicy:    segmentation.ClosePolicy{MaxBytes: 256},
		withCollection: true,
		pullFails:      1,
	})
	msgs := make([]ingestion.IngesterMessage, 8)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{
			Raw:   []byte("retry after pull failure"),
			Attrs: map[string]string{"env": "prod"},
		}
	}
	h.runIngester(t, &emitIngester{msgs: msgs})

	h.waitSyncs(t, 8)
	published := h.waitPublishedStable(t, 1)
	h.waitCollectedWithRetry(t, len(published))
	headPath := paths.HeadSegment(h.homeRoot, published[0].SegmentID)
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head on remote home: %v", err)
	}
}

func TestPipelineRemoteHomeFollowerBuildsWithoutSealChunk(t *testing.T) {
	// Replicated sealed manifest on a follower home builds GLCB locally but does not propose SealChunk.
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{
		closePolicy:    segmentation.ClosePolicy{MaxBytes: 256},
		withCollection: true,
		withChunking:   true,
		chunkLeader:    func() bool { return false },
	})
	msgs := make([]ingestion.IngesterMessage, 8)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{
			Raw:   []byte("follower home build"),
			Attrs: map[string]string{"env": "prod"},
		}
	}
	h.runIngester(t, &emitIngester{msgs: msgs})

	h.waitSyncs(t, 8)
	published := h.waitPublishedStable(t, 1)
	h.waitCollected(t, len(published))

	meta := published[0]
	openedAt := time.Now().UTC()
	applyFlowFSM(t, h.fsm, vaultctlfsm.MarshalOpenChunkManifest(h.chunkID, openedAt))
	applyFlowFSM(t, h.fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(h.chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         meta.SegmentID,
		FirstRecordNumber: 0,
		LastRecordNumber:  meta.RecordCount - 1,
		SliceBytes:        4096,
		RefAddedAt:        openedAt,
	}))
	sealedAt := openedAt.Add(time.Minute)
	applyFlowFSM(t, h.fsm, vaultctlfsm.MarshalSealOpenChunkManifest(h.chunkID, sealedAt))

	if err := h.chunk.BuildOnce(h.ctx, h.vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	glcbPath := chunking.ChunkGLCBPath(h.chunkRoot, h.chunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		t.Fatalf("follower GLCB: %v", err)
	}
	entry := h.fsm.Get(h.chunkID)
	if entry != nil && entry.State == chunk.ChunkStateSealed {
		t.Fatal("follower must not apply SealChunk to vault-ctl FSM")
	}
	if h.fsm.SealedManifest() == nil {
		t.Fatal("sealed manifest must remain until leader applies SealChunk")
	}
}

func TestPipelineRemoteHomePlannerRequiresLocalHead(t *testing.T) {
	// Vault leader planner on the remote home cannot add refs until collection lands segments locally.
	nodeID := glid.New()
	ingesterID := glid.New()
	vaultID := glid.New()

	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{
		closePolicy:    segmentation.ClosePolicy{MaxBytes: 256},
		withCollection: true,
		withChunking:   true,
		chunkPolicy:    chunking.ManifestRotationPolicy{MaxRecords: 100},
		gatedPull:      true,
	})
	msgs := make([]ingestion.IngesterMessage, 8)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{Raw: []byte("plan after collect"), Attrs: map[string]string{"k": "v"}}
	}
	h.runIngester(t, &emitIngester{msgs: msgs})

	h.waitSyncs(t, 8)
	published := h.waitPublishedStable(t, 1)

	if err := h.chunk.PlanOnce(h.ctx, h.vaultID); err != nil {
		t.Fatal(err)
	}
	if h.fsm.OpenChunk() != nil {
		t.Fatal("planner must not open manifest without local segment files")
	}

	h.gatePull.allow()
	h.waitCollectedWithRetry(t, len(published))
	// Each PlanOnce makes a single planner decision (open manifest, then one
	// ref per pass); FSM callbacks wake the async chunking worker for the
	// rest, so poll until refs land.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := h.chunk.PlanOnce(h.ctx, h.vaultID); err != nil {
			t.Fatal(err)
		}
		open := h.fsm.OpenChunk()
		if open != nil && len(open.Refs) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected open manifest with refs after collection")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// publishedLog implements collection.LogReader from distribution publish metadata.
type publishedLog struct {
	pub     *recordingPublisher
	vaultID glid.GLID
}

func (l *publishedLog) Roll(_ context.Context, vaultID glid.GLID) ([]collection.AssignedSegment, error) {
	if vaultID != l.vaultID {
		return nil, nil
	}
	l.pub.mu.Lock()
	defer l.pub.mu.Unlock()
	out := make([]collection.AssignedSegment, len(l.pub.published))
	for i, meta := range l.pub.published {
		out[i] = collection.AssignedSegment{
			VaultID:   meta.VaultID,
			SegmentID: meta.SegmentID,
			Checksum:  meta.Checksum,
		}
	}
	return out, nil
}

type flowCollectionReceipts struct {
	mu sync.Mutex
	n  int
}

func (r *flowCollectionReceipts) CommitHolderReceipt(_ context.Context, _ glid.GLID, _ glid.GLID) error {
	r.mu.Lock()
	r.n++
	r.mu.Unlock()
	return nil
}

func (r *flowCollectionReceipts) receiptCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

type flowDistributionPull struct {
	dist *distribution.Manager
}

func (p *flowDistributionPull) Pull(_ context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	return p.dist.ServePull(distribution.PullRequest{
		VaultID:   vaultID,
		SegmentID: segmentID,
		Dest:      dest,
	})
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
	inA, err := segMgr.RegisterVault(vaultA, rootA, segmentation.VaultConfig{})
	if err != nil {
		t.Fatal(err)
	}
	inB, err := segMgr.RegisterVault(vaultB, rootB, segmentation.VaultConfig{})
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
		Vaults: map[glid.GLID]chan<- segmentation.Input{
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
		entries, err := os.ReadDir(paths.WorkingDir(root))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("%s: working segments = %d", root, len(entries))
		}
		path := filepath.Join(paths.WorkingDir(root), entries[0].Name())
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

	h := newHarness(t, nodeID, ingesterID, vaultID, route, harnessOpts{})
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

	entries, err := os.ReadDir(paths.WorkingDir(h.vaultRoot))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments = %d", len(entries))
	}
	path := filepath.Join(paths.WorkingDir(h.vaultRoot), entries[0].Name())
	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	if sf.Header().RecordCount != 0 {
		t.Fatalf("segment record count = %d, want 0", sf.Header().RecordCount)
	}
}
