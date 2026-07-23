package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// These tests exercise the Rubicon C production collection types
// (segmentLogReader, segmentPullClient, segmentReceiptCommitter) wired to a
// real per-vault vault-ctl FSM and a real origin distribution.Manager. The
// cross-node byte transport (PullSegment RPC / *cluster.SegmentPuller) is
// covered separately in cluster/segment_puller_test.go; here the segmentPuller
// seam is a faithful in-process stand-in that streams the origin's served
// segment bytes, so we can drive multi-node distribution/collection semantics
// deterministically without standing up a Raft cluster.
//
// The FSM is shared between origin (publisher) and home (receipts): a single
// replicated state machine is exactly what Raft converges every node to, so a
// shared instance models the cross-node registry without the transport.

const (
	testOriginNode = "node-origin"
	testHomeNode   = "node-home"
)

// fsmApplier applies marshaled vault-ctl commands to a shared FSM, mirroring
// the leader-forwarding applier used in production (here the "leader" is the
// single shared FSM).
type fsmApplier struct {
	fsm *vaultctlfsm.FSM
}

func (a *fsmApplier) Apply(data []byte) error {
	cp := append([]byte(nil), data...)
	if r := a.fsm.Apply(&hraft.Log{Data: cp}); r != nil {
		if err, ok := r.(error); ok {
			return err
		}
		return fmt.Errorf("apply: %v", r)
	}
	return nil
}

// originPuller is the segmentPuller seam: it streams bytes from the origin's
// distribution.Manager.ServePull, optionally failing the first failsLeft
// attempts (writing a partial prefix first to prove the collector never
// promotes a partial segment to head/).
type originPuller struct {
	dist *distribution.Manager

	mu        sync.Mutex
	attempts  int
	failsLeft int
	// pulled counts attempts per segment so tests can assert exactly-once
	// pulls instead of comparing aggregate counters across a race window.
	pulled map[glid.GLID]int
}

func (p *originPuller) Pull(_ context.Context, nodeID string, vaultID, segmentID glid.GLID, dest io.Writer) error {
	p.mu.Lock()
	p.attempts++
	if p.pulled == nil {
		p.pulled = make(map[glid.GLID]int)
	}
	p.pulled[segmentID]++
	fail := p.failsLeft > 0
	if fail {
		p.failsLeft--
	}
	p.mu.Unlock()
	if fail {
		// Write a partial prefix then fail: segmentPullClient buffers, so dest
		// must never observe these bytes.
		_, _ = dest.Write([]byte("partial-bytes-before-failure"))
		return fmt.Errorf("origin %s unreachable", nodeID)
	}
	return p.dist.ServePull(distribution.PullRequest{VaultID: vaultID, SegmentID: segmentID, Dest: dest})
}

func (p *originPuller) attemptCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts
}

func (p *originPuller) pulledCounts() map[glid.GLID]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[glid.GLID]int, len(p.pulled))
	for id, n := range p.pulled {
		out[id] = n
	}
	return out
}

func (p *originPuller) setFails(n int) {
	p.mu.Lock()
	p.failsLeft = n
	p.mu.Unlock()
}

// originFixture is a running origin pipeline: segmentation -> distribution,
// publishing completed-segment metadata to the shared FSM.
type originFixture struct {
	vaultID glid.GLID
	root    string
	seg     *segmentation.Manager
	dist    *distribution.Manager
	in      chan<- segmentation.Input
	fsm     *vaultctlfsm.FSM
}

// completedSegments reports how many segments the origin's writer has promoted
// working/ -> completed/. The writer completes a segment and enqueues its
// publish BEFORE releasing that commit's acks (commitBatch.commit), so once
// every ingest ack has fired this count is final: no further segment can ever
// publish. That makes it the deterministic anchor for "the published set is
// complete" — unlike polling the FSM, which is satisfiable at any prefix of
// the publish stream while the last segment's publish is still in flight.
func (o *originFixture) completedSegments() uint64 {
	for _, st := range o.seg.AppendStats() {
		if st.VaultID == o.vaultID {
			return st.SegmentsCompleted
		}
	}
	return 0
}

// newOriginFixture starts an origin for vaultID that closes a segment every
// ~256 bytes and publishes it to fsm as testOriginNode.
func newOriginFixture(t *testing.T, ctx context.Context, vaultID glid.GLID, fsm *vaultctlfsm.FSM) *originFixture {
	t.Helper()
	root := t.TempDir()

	segMgr, completed := segmentation.New(segmentation.Config{
		CompletePolicy:  segmentation.CompletePolicy{MaxBytes: 256},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Millisecond,
	})
	in, err := segMgr.RegisterVault(vaultID, root, segmentation.VaultConfig{})
	if err != nil {
		t.Fatalf("origin RegisterVault segmentation: %v", err)
	}

	distMgr, _ := distribution.New(distribution.Config{})
	if err := distMgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher: &distribution.VaultCtlPublisher{
			Applier:      &fsmApplier{fsm: fsm},
			OriginNodeID: testOriginNode,
		},
		// Origin is not a placement home in these tests: it keeps the completed
		// segment locally and serves pulls.
		LocalHolder: func() bool { return false },
	}); err != nil {
		t.Fatalf("origin RegisterVault distribution: %v", err)
	}

	segDone := make(chan struct{})
	distDone := make(chan struct{})
	go func() { _ = segMgr.Run(ctx); close(segDone) }()
	go func() { _ = distMgr.Run(ctx, completed); close(distDone) }()
	t.Cleanup(func() {
		<-segDone
		<-distDone
	})

	return &originFixture{vaultID: vaultID, root: root, seg: segMgr, dist: distMgr, in: in, fsm: fsm}
}

// ingestAndPublish feeds an 8-record batch that closes and publishes one or
// more segments (the 256-byte complete policy typically yields several), waits
// for the first to appear in the FSM registry, and returns that segment's ID.
// Callers that assert on pull counts must quiesce until every completed
// segment is collected — waiting on just the returned ID races the remaining
// segments' first pulls (gastrolog-4fv63d). Because all acks have fired by
// return, completedSegments() is final; quiesce on that exact count, not on
// "all FSM entries seen so far", or a still-in-flight last publish races the
// assertion (gastrolog-3ly433).
func (o *originFixture) ingestAndPublish(t *testing.T, ctx context.Context) glid.GLID {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 8 {
		rec := record.Record{
			EventID: record.EventID{IngestTS: t0.Add(time.Duration(i) * time.Second)},
			Raw:     []byte("rubicon-c-segment-payload-line"),
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
		entries := o.fsm.ListCompletedSegments()
		if len(entries) >= 1 {
			return entries[0].SegmentID
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("origin did not publish a completed segment to the FSM")
	return glid.GLID{}
}

func segmentHolds(fsm *vaultctlfsm.FSM, segID glid.GLID, nodeID string) bool {
	entry := fsm.GetCompletedSegment(segID)
	if entry == nil {
		return false
	}
	return slices.Contains(entry.Holders, nodeID)
}

func headHas(t *testing.T, root string, segID glid.GLID) bool {
	t.Helper()
	_, err := os.Stat(paths.HeadSegment(root, segID))
	return err == nil
}

func headEmpty(t *testing.T, root string) bool {
	t.Helper()
	ids, err := paths.ListSegmentIDs(paths.HeadDir(root))
	if err != nil {
		t.Fatalf("list head: %v", err)
	}
	return len(ids) == 0
}

// TestPipelineCollectionReplicatesToRemoteHome: a segment produced and published by
// the origin is pulled into the remote home's head/ purely via the FSM publish
// fan-out (no timer, no manual nudge), the home is recorded as a holder, and a
// subsequent nudge neither re-pulls nor double-adds the holder (idempotent).
//
// The home node id differs from the origin: we assert replication from the
// perspective of a node that did NOT originate the segment.
func TestPipelineCollectionReplicatesToRemoteHome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)

	homeRoot := t.TempDir()
	puller := &originPuller{dist: origin.dist}
	colMgr := collection.New(collection.Config{})
	if err := colMgr.RegisterVault(vaultID, homeRoot, collection.VaultConfig{
		Log:      &segmentLogReader{lookup: func() *vaultctlfsm.FSM { return fsm }, localNodeID: testHomeNode},
		Pull:     &segmentPullClient{lookup: func() *vaultctlfsm.FSM { return fsm }, puller: puller, localNodeID: testHomeNode},
		Receipts: &segmentReceiptCommitter{applier: &fsmApplier{fsm: fsm}, localNodeID: testHomeNode},
		FSM:      fsm,
	}); err != nil {
		t.Fatalf("home RegisterVault collection: %v", err)
	}

	colDone := make(chan struct{})
	go func() { _ = colMgr.Run(ctx); close(colDone) }()
	t.Cleanup(func() { <-colDone })

	// Ensure Run is active (runCtx set) before the publish fires, so the
	// fan-out callback drives collection rather than a startup catch-up.
	settleCollectionStart()

	segID := origin.ingestAndPublish(t, ctx)

	// Callback-driven: no Notify/CollectOnce here.
	waitTrue(t, "segment replicated to remote home head/", func() bool {
		return headHas(t, homeRoot, segID)
	})
	waitTrue(t, "remote home recorded as holder", func() bool {
		return segmentHolds(fsm, segID, testHomeNode)
	})

	// The complete policy publishes several segments for the 8-record batch.
	// Every ingest ack has fired, so the writer's completed count is final
	// (see completedSegments) — anchor the quiesce to it. Requiring exactly
	// that many FSM entries closes the prefix race where the last segment's
	// publish lands after a "all entries seen so far are held" check passed
	// and its legitimate first pull reads as a "re-pull" (gastrolog-3ly433;
	// the earlier all-held quiesce from gastrolog-4fv63d could not tell
	// "done publishing" from "done so far").
	wantSegments := origin.completedSegments()
	if wantSegments == 0 {
		t.Fatal("origin completed no segments; the 8-record batch should close several")
	}
	waitTrue(t, "every completed segment published, collected, and receipted", func() bool {
		entries := fsm.ListCompletedSegments()
		if uint64(len(entries)) != wantSegments {
			return false
		}
		for _, e := range entries {
			if !slices.Contains(e.Holders, testHomeNode) {
				return false
			}
		}
		return true
	})

	// Idempotency: a redundant nudge must not re-pull or double-add holders.
	attemptsAfterCollect := puller.attemptCount()
	colMgr.Notify(vaultID)
	time.Sleep(50 * time.Millisecond)
	if got := puller.attemptCount(); got != attemptsAfterCollect {
		t.Fatalf("redundant nudge re-pulled: attempts %d -> %d", attemptsAfterCollect, got)
	}
	for id, n := range puller.pulledCounts() {
		if n != 1 {
			t.Fatalf("segment %s pulled %d times, want exactly 1", id, n)
		}
	}
	for _, e := range fsm.ListCompletedSegments() {
		n := 0
		for _, h := range e.Holders {
			if h == testHomeNode {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("home appears %d times in holders %v for segment %s, want 1", n, e.Holders, e.SegmentID)
		}
	}
}

// TestPipelineCollectionPullFailureLeavesNoPartialHead: while the origin is
// unreachable, a collect pass fails, leaves nothing in head/, and does not
// record the home as a holder. Once reachable, a retry pulls, verifies,
// promotes to head/, and records the receipt. Drives collection synchronously
// via CollectOnce (Run not started) for determinism.
func TestPipelineCollectionPullFailureLeavesNoPartialHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)

	homeRoot := t.TempDir()
	puller := &originPuller{dist: origin.dist, failsLeft: 1 << 30} // unreachable
	colMgr := collection.New(collection.Config{})
	if err := colMgr.RegisterVault(vaultID, homeRoot, collection.VaultConfig{
		Log:      &segmentLogReader{lookup: func() *vaultctlfsm.FSM { return fsm }, localNodeID: testHomeNode},
		Pull:     &segmentPullClient{lookup: func() *vaultctlfsm.FSM { return fsm }, puller: puller, localNodeID: testHomeNode},
		Receipts: &segmentReceiptCommitter{applier: &fsmApplier{fsm: fsm}, localNodeID: testHomeNode},
		// No FSM wiring: this test owns the collect cadence via CollectOnce.
	}); err != nil {
		t.Fatalf("home RegisterVault collection: %v", err)
	}

	segID := origin.ingestAndPublish(t, ctx)

	// Origin unreachable: collect fails, no partial head, no holder receipt.
	if err := colMgr.CollectOnce(ctx, vaultID); err == nil {
		t.Fatal("expected collect to fail while origin is unreachable")
	}
	if !headEmpty(t, homeRoot) {
		t.Fatal("head/ must be empty after a failed pull (no partial promote)")
	}
	if segmentHolds(fsm, segID, testHomeNode) {
		t.Fatal("home must not be a holder after a failed pull")
	}

	// Origin reachable: retry pulls, verifies, promotes, and records receipt.
	puller.setFails(0)
	if err := colMgr.CollectOnce(ctx, vaultID); err != nil {
		t.Fatalf("collect after recovery: %v", err)
	}
	if !headHas(t, homeRoot, segID) {
		t.Fatal("segment not promoted to head/ after recovery")
	}
	if !segmentHolds(fsm, segID, testHomeNode) {
		t.Fatal("home not recorded as holder after recovery")
	}
}

// TestPipelineCollectionRecoversFromUnreachableOriginViaRetries: transient origin
// failures are tolerated — collection keeps retrying (here via nudges) until
// the pull succeeds and the segment lands in head/.
func TestPipelineCollectionRecoversFromUnreachableOriginViaRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)

	homeRoot := t.TempDir()
	puller := &originPuller{dist: origin.dist, failsLeft: 3} // 3 transient failures
	colMgr := collection.New(collection.Config{})
	if err := colMgr.RegisterVault(vaultID, homeRoot, collection.VaultConfig{
		Log:      &segmentLogReader{lookup: func() *vaultctlfsm.FSM { return fsm }, localNodeID: testHomeNode},
		Pull:     &segmentPullClient{lookup: func() *vaultctlfsm.FSM { return fsm }, puller: puller, localNodeID: testHomeNode},
		Receipts: &segmentReceiptCommitter{applier: &fsmApplier{fsm: fsm}, localNodeID: testHomeNode},
		FSM:      fsm,
	}); err != nil {
		t.Fatalf("home RegisterVault collection: %v", err)
	}
	colDone := make(chan struct{})
	go func() { _ = colMgr.Run(ctx); close(colDone) }()
	t.Cleanup(func() { <-colDone })
	settleCollectionStart()

	segID := origin.ingestAndPublish(t, ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !headHas(t, homeRoot, segID) {
		colMgr.Notify(vaultID)
		time.Sleep(20 * time.Millisecond)
	}
	if !headHas(t, homeRoot, segID) {
		t.Fatalf("segment never recovered into head/ after transient failures (attempts=%d)", puller.attemptCount())
	}
	// The holder receipt commits as a batch at the END of the collect pass
	// (gastrolog-38snf4), so head/ becomes visible slightly before the FSM
	// records the holder — wait rather than asserting instantly.
	waitTrue(t, "home recorded as holder after recovery", func() bool {
		return segmentHolds(fsm, segID, testHomeNode)
	})
}

// TestPipelineSegmentLogReaderSkipsHeldSegments: Roll returns only segments the local
// node does not already hold, so a home stops re-assigning a segment once its
// holder receipt has replicated.
func TestPipelineSegmentLogReaderSkipsHeldSegments(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segHeld := glid.New()
	segOpen := glid.New()

	for _, sid := range []glid.GLID{segHeld, segOpen} {
		if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
			SegmentID:    sid,
			RecordCount:  1,
			OriginNodeID: testOriginNode,
		})); err != nil {
			t.Fatalf("publish %s: %v", sid, err)
		}
	}
	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segHeld, testHomeNode)); err != nil {
		t.Fatalf("ack holder: %v", err)
	}

	reader := &segmentLogReader{lookup: func() *vaultctlfsm.FSM { return fsm }, localNodeID: testHomeNode}
	assigned, err := reader.Roll(context.Background(), vaultID)
	if err != nil {
		t.Fatalf("Roll: %v", err)
	}
	if len(assigned) != 1 || assigned[0].SegmentID != segOpen {
		t.Fatalf("Roll = %+v, want only %s (the unheld segment)", assigned, segOpen)
	}
}

// TestPipelineSegmentLogReaderReassignsHeldWhenLocalMissing: a holder receipt
// without local bytes must be re-assigned when chunking still needs records.
func TestPipelineSegmentLogReaderReassignsHeldWhenLocalMissing(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    segID,
		RecordCount:  10,
		OriginNodeID: testOriginNode,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, testHomeNode)); err != nil {
		t.Fatalf("ack holder: %v", err)
	}
	chunkID := chunk.NewChunkID()
	if err := applier.Apply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, time.Unix(0, 1_700_000_000_000).UTC())); err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	// Partial chunking: resume at 5, bytes purged from head/.
	if err := applier.Apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 4,
		SliceBytes: 50, RefAddedAt: time.Unix(0, 1_700_000_000_000).UTC(),
	})); err != nil {
		t.Fatalf("add ref: %v", err)
	}

	reader := &segmentLogReader{
		lookup:      func() *vaultctlfsm.FSM { return fsm },
		localNodeID: testHomeNode,
		vaultRoot:   root,
	}
	assigned, err := reader.Roll(context.Background(), vaultID)
	if err != nil {
		t.Fatalf("Roll: %v", err)
	}
	if len(assigned) != 1 || assigned[0].SegmentID != segID {
		t.Fatalf("Roll = %+v, want %s reassigned for re-pull", assigned, segID)
	}
}

// TestPipelineSegmentLogReaderSkipsSupersededSegment (R4): a segment whose
// records live in an RF-replicated chunk is superseded — Roll must not assign
// it for pull even though this node holds neither the segment nor the chunk.
// The node catches up at the chunk level, not by re-pulling purged transport.
func TestPipelineSegmentLogReaderSkipsSupersededSegment(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segID := glid.New()
	chunkID := chunk.NewChunkID()
	now := time.Unix(0, 1_700_000_000_000).UTC()

	mustApply := func(data []byte) {
		t.Helper()
		if err := applier.Apply(data); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	mustApply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, OriginNodeID: testOriginNode,
	}))
	mustApply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, now))
	mustApply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1, RefAddedAt: now,
	}))
	mustApply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))
	mustApply(vaultctlfsm.MarshalSealChunk(chunkID, now.Add(time.Minute), 1, 1, now, now, now, true, now.Add(time.Minute)))
	for _, node := range []string{"chunk-holder-a", "chunk-holder-b"} {
		data, err := vaultctlfsm.MarshalAckChunkHolders([]chunk.ChunkID{chunkID}, node)
		if err != nil {
			t.Fatalf("marshal ack chunk holders: %v", err)
		}
		mustApply(data)
	}

	// placement of 3 → RF threshold min(2,3)=2, reached by the two chunk holders.
	placement := func() []string { return []string{"chunk-holder-a", "chunk-holder-b", testHomeNode} }
	reader := &segmentLogReader{
		lookup:      func() *vaultctlfsm.FSM { return fsm },
		localNodeID: testHomeNode,
		placement:   placement,
	}
	assigned, err := reader.Roll(context.Background(), vaultID)
	if err != nil {
		t.Fatalf("Roll: %v", err)
	}
	if len(assigned) != 0 {
		t.Fatalf("Roll = %+v, want empty (superseded segment must not be pulled)", assigned)
	}

	// Without placement wiring the skip is disabled and the unheld segment is
	// assigned as before — proves the skip is what suppresses it.
	reader.placement = nil
	assigned, err = reader.Roll(context.Background(), vaultID)
	if err != nil {
		t.Fatalf("Roll (no placement): %v", err)
	}
	if len(assigned) != 1 || assigned[0].SegmentID != segID {
		t.Fatalf("Roll (no placement) = %+v, want the segment assigned", assigned)
	}
}

// TestPipelineSegmentLogReaderReassignsWhenSealedManifestNeedsBytes: when the
// planner fully consumed a segment but GLCB build still references it in the
// sealed-pending manifest, Roll must re-assign the segment for re-pull even
// though the resume cursor reached RecordCount and a holder receipt exists.
func TestPipelineSegmentLogReaderReassignsWhenSealedManifestNeedsBytes(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	const recordCount uint32 = 10

	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    segID,
		RecordCount:  recordCount,
		OriginNodeID: testOriginNode,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, testHomeNode)); err != nil {
		t.Fatalf("ack holder: %v", err)
	}
	chunkID := chunk.NewChunkID()
	openedAt := time.Unix(0, 1_700_000_000_000).UTC()
	if err := applier.Apply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt)); err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  recordCount - 1,
		SliceBytes:        100,
		RefAddedAt:        openedAt,
	})); err != nil {
		t.Fatalf("add ref: %v", err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, openedAt.Add(time.Minute))); err != nil {
		t.Fatalf("seal open manifest: %v", err)
	}
	if next, ok := fsm.ResumeRecordNumber(segID); !ok || next != recordCount {
		t.Fatalf("ResumeRecordNumber = (%d, %v), want (%d, true)", next, ok, recordCount)
	}

	reader := &segmentLogReader{
		lookup:      func() *vaultctlfsm.FSM { return fsm },
		localNodeID: testHomeNode,
		vaultRoot:   root,
	}
	assigned, err := reader.Roll(context.Background(), vaultID)
	if err != nil {
		t.Fatalf("Roll: %v", err)
	}
	if len(assigned) != 1 || assigned[0].SegmentID != segID {
		t.Fatalf("Roll = %+v, want [%s] for sealed-manifest build recovery", assigned, segID)
	}
}

// TestSegmentPullClientReadsLocalOriginBeforeRPC: when this home originated
// the segment, collection must read completed/ (or head/) locally instead of
// RPC-pulling from self as origin — the registry publish wake can run before
// distribution promotes the file into head/.
func TestSegmentPullClientReadsLocalOriginBeforeRPC(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		t.Fatal(err)
	}
	const want = "local-origin-segment-bytes"
	if err := os.WriteFile(paths.CompletedSegment(root, segID), []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    segID,
		RecordCount:  1,
		OriginNodeID: testHomeNode,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	fake := &recordingPuller{serve: map[string]string{"should-not-run": "nope"}}
	client := &segmentPullClient{
		lookup:      func() *vaultctlfsm.FSM { return fsm },
		puller:      fake,
		localNodeID: testHomeNode,
		vaultRoot:   root,
	}
	var dest writeCounter
	if err := client.Pull(context.Background(), vaultID, segID, &dest); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(fake.order) != 0 {
		t.Fatalf("RPC pull order = %v, want none (local read)", fake.order)
	}
	if dest.String() != want {
		t.Fatalf("dest = %q, want %q", dest.String(), want)
	}
}

// TestPipelineSegmentPullClientResolvesSourcesAndBuffers: the pull client tries the
// origin first, falls back to another holder when the origin is unreachable,
// never pulls from the local node, and never leaks bytes from a failed source
// into dest (it buffers each candidate and copies only on success).
func TestPipelineSegmentPullClientResolvesSourcesAndBuffers(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segID := glid.New()

	// Entry: origin "node-origin"; holders include the local node and a remote
	// holder "node-holder". Source order must be origin, then node-holder, with
	// the local node skipped.
	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    segID,
		RecordCount:  1,
		OriginNodeID: testOriginNode,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for _, n := range []string{testHomeNode, "node-holder"} {
		if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, n)); err != nil {
			t.Fatalf("ack %s: %v", n, err)
		}
	}

	const goodBytes = "the-real-segment-bytes"
	fake := &recordingPuller{
		serve: map[string]string{"node-holder": goodBytes},
		fail:  map[string]bool{testOriginNode: true},
	}
	client := &segmentPullClient{lookup: func() *vaultctlfsm.FSM { return fsm }, puller: fake, localNodeID: testHomeNode}

	var dest writeCounter
	if err := client.Pull(context.Background(), vaultID, segID, &dest); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	// Origin attempted first (and failed), then node-holder; local node skipped.
	want := []string{testOriginNode, "node-holder"}
	if !slices.Equal(fake.order, want) {
		t.Fatalf("source order = %v, want %v (origin first, local skipped)", fake.order, want)
	}
	// dest received only the successful source's bytes — no partial prefix from
	// the failed origin.
	if dest.String() != goodBytes {
		t.Fatalf("dest = %q, want %q (no partial bytes from failed source)", dest.String(), goodBytes)
	}
}

// TestPipelineSegmentPullClientStreamsToFileTruncatingFailures: with a
// rewindable dest (the production pre-head temp file), the pull client
// streams each candidate directly into the file — no whole-segment RAM
// buffer (gastrolog-1xee1s) — and truncates partial bytes from a failed
// source before trying the next one.
func TestPipelineSegmentPullClientStreamsToFileTruncatingFailures(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segID := glid.New()

	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    segID,
		RecordCount:  1,
		OriginNodeID: testOriginNode,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, "node-holder")); err != nil {
		t.Fatalf("ack: %v", err)
	}

	const goodBytes = "the-real-segment-bytes"
	fake := &recordingPuller{
		serve: map[string]string{"node-holder": goodBytes},
		fail:  map[string]bool{testOriginNode: true},
	}
	client := &segmentPullClient{lookup: func() *vaultctlfsm.FSM { return fsm }, puller: fake, localNodeID: testHomeNode}

	path := filepath.Join(t.TempDir(), "seg.pulling")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := client.Pull(context.Background(), vaultID, segID, f); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	want := []string{testOriginNode, "node-holder"}
	if !slices.Equal(fake.order, want) {
		t.Fatalf("source order = %v, want %v", fake.order, want)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The failed origin wrote a partial prefix straight into the file; the
	// client must have truncated it before streaming the holder's bytes.
	if string(got) != goodBytes {
		t.Fatalf("file = %q, want %q (partial bytes from failed source not truncated)", got, goodBytes)
	}
}

// TestPipelineSegmentPullClientAllSourcesFailLeavesEmptyFile: when every
// source fails mid-stream, the rewindable dest ends truncated to its
// starting offset — no partial bytes survive for the caller to promote.
func TestPipelineSegmentPullClientAllSourcesFailLeavesEmptyFile(t *testing.T) {
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	segID := glid.New()

	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    segID,
		RecordCount:  1,
		OriginNodeID: testOriginNode,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	fake := &recordingPuller{fail: map[string]bool{testOriginNode: true}}
	client := &segmentPullClient{lookup: func() *vaultctlfsm.FSM { return fsm }, puller: fake, localNodeID: testHomeNode}

	path := filepath.Join(t.TempDir(), "seg.pulling")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := client.Pull(context.Background(), vaultID, segID, f); err == nil {
		t.Fatal("Pull succeeded, want error (all sources fail)")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("file size = %d, want 0 (partial bytes must be truncated)", info.Size())
	}
}

// recordingPuller records pull order and serves/fails per node, writing a
// partial prefix before failing to verify the client discards failed bytes.
type recordingPuller struct {
	order []string
	serve map[string]string
	fail  map[string]bool
}

func (p *recordingPuller) Pull(_ context.Context, nodeID string, _, _ glid.GLID, dest io.Writer) error {
	p.order = append(p.order, nodeID)
	if p.fail[nodeID] {
		_, _ = dest.Write([]byte("partial-from-failed-source"))
		return fmt.Errorf("node %s unreachable", nodeID)
	}
	_, _ = io.WriteString(dest, p.serve[nodeID])
	return nil
}

type writeCounter struct {
	buf []byte
}

func (w *writeCounter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *writeCounter) String() string { return string(w.buf) }

// settleCollectionStart lets a freshly-launched Run loop set its runCtx before
// the origin publishes, so the publish fan-out callback (which is a no-op until
// runCtx is set) is delivered. Run assigns runCtx in a short synchronized
// section at the top of the goroutine; the multi-stage async ingest that
// follows this settle takes far longer, so the callback always lands after Run
// is active. This is test-setup timing, not production polling — the manager
// itself never polls on a timer.
func settleCollectionStart() {
	time.Sleep(100 * time.Millisecond)
}

func waitTrue(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// TestSegmentPullClientAttachesUnavailableSentinel pins the boundary
// translation in the segment pull client (gastrolog-466kq5): registry and
// holder-resolution misses carry collection.ErrSegmentUnavailable so retry
// classification runs on errors.Is — never on these messages' prose.
func TestSegmentPullClientAttachesUnavailableSentinel(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	vaultID := glid.New()
	client := &segmentPullClient{lookup: func() *vaultctlfsm.FSM { return fsm }, puller: &recordingPuller{}, localNodeID: testHomeNode}

	// Segment not yet in the vault-ctl registry (local FSM behind the publish).
	var dest writeCounter
	err := client.Pull(context.Background(), vaultID, glid.New(), &dest)
	if !errors.Is(err, collection.ErrSegmentUnavailable) {
		t.Fatalf("registry miss must carry ErrSegmentUnavailable, got: %v", err)
	}

	// Registered segment whose only known source is the local node itself:
	// no remote holder to pull from until another ack lands.
	segID := glid.New()
	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    segID,
		RecordCount:  1,
		OriginNodeID: testHomeNode,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	err = client.Pull(context.Background(), vaultID, segID, &dest)
	if !errors.Is(err, collection.ErrSegmentUnavailable) {
		t.Fatalf("no-remote-holder must carry ErrSegmentUnavailable, got: %v", err)
	}
}

// TestTranslateServePullError pins the server-side boundary mapping against
// the real distribution sentinels it must translate (gastrolog-466kq5):
// "cannot serve this segment here" becomes collection.ErrSegmentUnavailable
// (encoded as NotFound on the wire); genuine serving faults pass through
// unchanged so the pulling home logs them at Warn instead of deferring.
func TestTranslateServePullError(t *testing.T) {
	t.Parallel()

	if translateServePullError(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	unavailable := []error{
		distribution.ErrSegmentNotFound,
		distribution.ErrSegmentGone,
		distribution.ErrUnknownVault,
		fmt.Errorf("serve pull: %w", distribution.ErrSegmentNotFound),
	}
	for _, err := range unavailable {
		got := translateServePullError(err)
		if !errors.Is(got, collection.ErrSegmentUnavailable) {
			t.Errorf("translate(%v) must carry ErrSegmentUnavailable, got: %v", err, got)
		}
		if !errors.Is(got, err) {
			t.Errorf("translate(%v) must keep the cause in the chain, got: %v", err, got)
		}
	}

	fault := errors.New("read segment file: input/output error")
	if got := translateServePullError(fault); got != fault {
		t.Errorf("serving fault must pass through unchanged, got: %v", got)
	}
}
