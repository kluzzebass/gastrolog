package pipeline

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// --- test doubles ---

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

func (p *recordingPublisher) assigned(vaultID glid.GLID) []collection.AssignedSegment {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]collection.AssignedSegment, 0, len(p.published))
	for _, meta := range p.published {
		out = append(out, collection.AssignedSegment{
			VaultID:   meta.VaultID,
			SegmentID: meta.SegmentID,
			Checksum:  meta.Checksum,
		})
	}
	return out
}

// fsmApplier applies marshaled vault-ctl commands to an in-process FSM.
type fsmApplier struct {
	fsm *vaultctlfsm.FSM
}

func (a *fsmApplier) Apply(data []byte) error {
	cp := append([]byte(nil), data...)
	if result := a.fsm.Apply(&hraft.Log{Data: cp}); result != nil {
		if err, ok := result.(error); ok {
			return err
		}
	}
	return nil
}

// vaultCtlPublisher records metadata and commits the segment registry entry to FSM.
type vaultCtlPublisher struct {
	rec  *recordingPublisher
	vctl *distribution.VaultCtlPublisher
}

func (p *vaultCtlPublisher) Publish(ctx context.Context, meta distribution.Metadata) error {
	if err := p.rec.Publish(ctx, meta); err != nil {
		return err
	}
	return p.vctl.Publish(ctx, meta)
}

// publishedLog derives the assignment list from published segment metadata.
type publishedLog struct {
	pub     *recordingPublisher
	vaultID glid.GLID
}

func (l *publishedLog) Roll(_ context.Context, vaultID glid.GLID) ([]collection.AssignedSegment, error) {
	if vaultID != l.vaultID {
		return nil, nil
	}
	return l.pub.assigned(vaultID), nil
}

// supervisorPull pulls segments back through the supervisor's own ServePull seam.
type supervisorPull struct {
	sup *Supervisor
}

func (p *supervisorPull) Pull(_ context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	return p.sup.ServePull(distribution.PullRequest{
		VaultID:   vaultID,
		SegmentID: segmentID,
		Dest:      dest,
	})
}

type countingReceipts struct {
	mu sync.Mutex
	n  int
}

func (r *countingReceipts) CommitHolderReceipts(_ context.Context, _ glid.GLID, segmentIDs []glid.GLID) error {
	r.mu.Lock()
	r.n += len(segmentIDs)
	r.mu.Unlock()
	return nil
}

func (r *countingReceipts) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// --- helpers ---

func allRoute(t *testing.T, vaultID glid.GLID) *routing.Table {
	t.Helper()
	route, err := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	return routing.NewTable([]*routing.Route{route})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func batch(n int, raw string) []ingestion.IngesterMessage {
	msgs := make([]ingestion.IngesterMessage, n)
	for i := range msgs {
		msgs[i] = ingestion.IngesterMessage{
			Raw:   []byte(raw),
			Attrs: map[string]string{"env": "prod"},
		}
	}
	return msgs
}

// --- tests ---

func TestSupervisorStartStopNoVaults(t *testing.T) {
	sup := New(Config{NodeID: glid.New()})

	if err := sup.Stop(); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Stop before Start = %v, want ErrNotRunning", err)
	}

	ctx := context.Background()
	if err := sup.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := sup.Start(ctx); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Start = %v, want ErrAlreadyRunning", err)
	}

	if err := sup.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := sup.Stop(); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("second Stop = %v, want ErrNotRunning", err)
	}
}

func TestSupervisorRegisterValidation(t *testing.T) {
	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, glid.New())})

	if err := sup.RegisterVault(VaultSpec{}); err == nil {
		t.Fatal("register with zero ID should fail")
	}
	if err := sup.RegisterVault(VaultSpec{VaultID: glid.New()}); err == nil {
		t.Fatal("register with no roles should fail")
	}

	vaultID := glid.New()
	pub := &recordingPublisher{}
	spec := VaultSpec{VaultID: vaultID, Origin: true, OriginRoot: t.TempDir(), Publisher: pub}
	if err := sup.RegisterVault(spec); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := sup.RegisterVault(spec); !errors.Is(err, ErrVaultRegistered) {
		t.Fatalf("second register = %v, want ErrVaultRegistered", err)
	}
}

// TestSupervisorOriginIngestToPublish exercises the write half end to end through
// the supervisor: ingestion → digestion → routing (dynamic register) →
// segmentation → distribution publish.
func TestSupervisorOriginIngestToPublish(t *testing.T) {
	nodeID := glid.New()
	vaultID := glid.New()

	sup := New(Config{
		NodeID:             nodeID,
		Table:              allRoute(t, vaultID),
		SegmentClosePolicy: segmentation.ClosePolicy{MaxBytes: 256},
	})
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop() })

	pub := &recordingPublisher{}
	if err := sup.RegisterVault(VaultSpec{
		VaultID:    vaultID,
		Origin:     true,
		OriginRoot: t.TempDir(),
		Publisher:  pub,
	}); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	if err := sup.ReconcileIngesters([]ingestion.IngesterSpec{
		{ID: glid.New(), Ingester: &emitIngester{msgs: batch(8, "origin payload line")}, Name: "t", Type: "mock"},
	}); err != nil {
		t.Fatalf("ReconcileIngesters: %v", err)
	}

	waitFor(t, "published segment", func() bool { return pub.count() >= 1 })
}

// TestSupervisorFullVaultHome registers a vault as both Origin and Home on one
// node and drives the full graph: published segments are collected back through
// the supervisor's pull seam, exercising all seven managers.
func TestSupervisorFullVaultHome(t *testing.T) {
	nodeID := glid.New()
	vaultID := glid.New()
	homeRoot := t.TempDir()

	sup := New(Config{
		NodeID:             nodeID,
		Table:              allRoute(t, vaultID),
		SegmentClosePolicy: segmentation.ClosePolicy{MaxBytes: 256},
	})
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop() })

	fsm := vaultctlfsm.New()
	rec := &recordingPublisher{}
	pub := &vaultCtlPublisher{
		rec: rec,
		vctl: &distribution.VaultCtlPublisher{
			Applier:      &fsmApplier{fsm: fsm},
			OriginNodeID: nodeID.String(),
		},
	}
	receipts := &countingReceipts{}

	spec := VaultSpec{
		VaultID:     vaultID,
		Origin:      true,
		OriginRoot:  t.TempDir(),
		Publisher:   pub,
		Home:        true,
		HomeRoot:    homeRoot,
		ChunkRoot:   filepath.Join(homeRoot, "chunks"),
		FSM:         fsm,
		Log:         &publishedLog{pub: rec, vaultID: vaultID},
		Pull:        &supervisorPull{sup: sup},
		Receipts:    receipts,
		Locate:      chunking.VaultSegmentLocator{Root: homeRoot},
		Applier:     &fsmApplier{fsm: fsm},
		IsLeader:    func() bool { return true },
		ChunkPolicy: chunking.ManifestRotationPolicy{MaxRecords: 8},
		NewChunkID:  func() chunk.ChunkID { return chunk.NewChunkID() },
	}
	if err := sup.RegisterVault(spec); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	if err := sup.ReconcileIngesters([]ingestion.IngesterSpec{
		{ID: glid.New(), Ingester: &emitIngester{msgs: batch(8, "home payload line")}, Name: "t", Type: "mock"},
	}); err != nil {
		t.Fatalf("ReconcileIngesters: %v", err)
	}

	waitFor(t, "published segment", func() bool { return rec.count() >= 1 })

	// Drive a collection pass via the orchestrator seam: the segment published by
	// the origin half is rolled from the log, pulled back through ServePull,
	// verified, promoted to head, and a holder receipt is committed. This exercises
	// collection (and the supervisor pull seam) for the registered vault home.
	if err := sup.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatalf("CollectOnce: %v", err)
	}
	waitFor(t, "holder receipt", func() bool { return receipts.count() >= 1 })
}

// TestSupervisorReconcilePlacementFlap simulates a placement change moving a vault
// off and back onto this node, asserting clean unregister and re-register of all
// managers.
func TestSupervisorReconcilePlacementFlap(t *testing.T) {
	vaultID := glid.New()
	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, vaultID)})
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop() })

	homeRoot := t.TempDir()
	fsm := vaultctlfsm.New()
	newSpec := func() VaultSpec {
		return VaultSpec{
			VaultID:    vaultID,
			Origin:     true,
			OriginRoot: t.TempDir(),
			Publisher:  &recordingPublisher{},
			Home:       true,
			HomeRoot:   homeRoot,
			ChunkRoot:  filepath.Join(homeRoot, "chunks"),
			FSM:        fsm,
			Log:        &publishedLog{pub: &recordingPublisher{}, vaultID: vaultID},
			Pull:       &supervisorPull{sup: sup},
			Receipts:   &countingReceipts{},
			Locate:     chunking.VaultSegmentLocator{Root: homeRoot},
			Applier:    &fsmApplier{fsm: fsm},
			IsLeader:   func() bool { return false },
		}
	}

	if err := sup.RegisterVault(newSpec()); err != nil {
		t.Fatalf("initial register: %v", err)
	}
	// Vault leaves this node.
	sup.UnregisterVault(vaultID)
	// Unregister of an unknown vault is a no-op.
	sup.UnregisterVault(glid.New())
	// Vault returns; re-registration must succeed against the same managers.
	if err := sup.RegisterVault(newSpec()); err != nil {
		t.Fatalf("re-register after flap: %v", err)
	}
}

func TestSupervisorReleasePurgesOriginAndHomeHead(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	originRoot := t.TempDir()
	homeRoot := t.TempDir()
	fsm := vaultctlfsm.New()

	if err := paths.EnsureHeadDir(originRoot); err != nil {
		t.Fatal(err)
	}
	if err := paths.EnsureHeadDir(homeRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.HeadSegment(originRoot, segID), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.HeadSegment(homeRoot, segID), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, vaultID)})
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop() })

	spec := VaultSpec{
		VaultID:    vaultID,
		Origin:     true,
		OriginRoot: originRoot,
		Publisher:  &recordingPublisher{},
		HomeRoot:   homeRoot,
		ChunkRoot:  filepath.Join(homeRoot, "chunks"),
		FSM:        fsm,
		Locate:     chunking.VaultSegmentLocator{Root: homeRoot},
		Applier:    &fsmApplier{fsm: fsm},
		IsLeader:   func() bool { return true },
	}
	if err := sup.RegisterVault(spec); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
	})}); err != nil {
		t.Fatalf("PublishCompletedSegment: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalReleaseSegments([]glid.GLID{segID})}); err != nil {
		t.Fatalf("ReleaseSegments: %v", err)
	}
	if _, err := os.Stat(paths.HeadSegment(originRoot, segID)); !os.IsNotExist(err) {
		t.Fatalf("origin head should be purged, stat err=%v", err)
	}
	if _, err := os.Stat(paths.HeadSegment(homeRoot, segID)); !os.IsNotExist(err) {
		t.Fatalf("home head should be purged, stat err=%v", err)
	}
}

func TestStagingHeadPurgeRootsDedupesSharedRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	got := stagingHeadPurgeRoots(root, root, true, true)
	if len(got) != 1 || got[0] != root {
		t.Fatalf("stagingHeadPurgeRoots = %v, want [%s]", got, root)
	}
}

// TestSupervisorAdmissionGateRejects pins the admission contract on both
// external intakes: a gate error rejects the record with that error, and a
// nil/admitting gate changes nothing. (The ingester-pump intake nacks the
// same way; it shares admit().)
func TestSupervisorAdmissionGateRejects(t *testing.T) {
	rejected := errors.New("node is out of disk space")
	gateErr := rejected
	sup := New(Config{
		NodeID: glid.New(),
		Table:  allRoute(t, glid.New()),
		AdmissionGate: func() error {
			return gateErr
		},
	})
	ctx := context.Background()
	if err := sup.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sup.Stop() }()

	rec := &record.Record{Raw: []byte("x")}
	if err := sup.Submit(ctx, routing.IngestInput(rec)); !errors.Is(err, rejected) {
		t.Fatalf("Submit under gate = %v, want rejection", err)
	}
	if err := sup.SubmitToVault(ctx, glid.New(), rec, nil); !errors.Is(err, rejected) {
		t.Fatalf("SubmitToVault under gate = %v, want rejection", err)
	}

	// Gate lifts: intake resumes (vault unknown is fine — admission ran first).
	gateErr = nil
	if err := sup.Submit(ctx, routing.IngestInput(rec)); err != nil {
		t.Fatalf("Submit after gate lift: %v", err)
	}
}
