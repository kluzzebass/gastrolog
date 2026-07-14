package collection_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

type staticLog struct {
	mu        sync.Mutex
	assigned  []collection.AssignedSegment
	rollCalls int
}

func (l *staticLog) Roll(_ context.Context, _ glid.GLID) ([]collection.AssignedSegment, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rollCalls++
	out := make([]collection.AssignedSegment, len(l.assigned))
	copy(out, l.assigned)
	return out, nil
}

func (l *staticLog) setAssigned(segs ...collection.AssignedSegment) {
	l.mu.Lock()
	l.assigned = append([]collection.AssignedSegment(nil), segs...)
	l.mu.Unlock()
}

type memoryPull struct {
	mu   sync.Mutex
	data map[glid.GLID][]byte
}

func newMemoryPull() *memoryPull {
	return &memoryPull{data: make(map[glid.GLID][]byte)}
}

func (p *memoryPull) Put(segmentID glid.GLID, data []byte) {
	p.mu.Lock()
	p.data[segmentID] = append([]byte(nil), data...)
	p.mu.Unlock()
}

func (p *memoryPull) Pull(_ context.Context, _ glid.GLID, segmentID glid.GLID, dest io.Writer) error {
	p.mu.Lock()
	data := p.data[segmentID]
	p.mu.Unlock()
	if data == nil {
		return io.ErrUnexpectedEOF
	}
	_, err := dest.Write(data)
	return err
}

type recordingReceipts struct {
	mu       sync.Mutex
	receipts []glid.GLID
}

func (r *recordingReceipts) CommitHolderReceipts(_ context.Context, _ glid.GLID, segmentIDs []glid.GLID) error {
	r.mu.Lock()
	r.receipts = append(r.receipts, segmentIDs...)
	r.mu.Unlock()
	return nil
}

func (r *recordingReceipts) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.receipts)
}

func TestCollectOncePullsMissingSegment(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	segBytes := writeSegmentBytes(t, vaultID, segID, "collected")
	pull := newMemoryPull()
	pull.Put(segID, segBytes)

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     pull,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}

	headPath := paths.HeadSegment(root, segID)
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head file: %v", err)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1", receipts.count())
	}
}

func TestCollectSegmentsPullsByID(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	segBytes := writeSegmentBytes(t, vaultID, segID, "targeted-collect")
	pull := newMemoryPull()
	pull.Put(segID, segBytes)
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      &staticLog{},
		Pull:     pull,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	if err := mgr.CollectSegments(context.Background(), vaultID, []glid.GLID{segID}); err != nil {
		t.Fatal(err)
	}
	headPath := paths.HeadSegment(root, segID)
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head file: %v", err)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1", receipts.count())
	}
}

// TestCollectOnceReceiptsSegmentAlreadyInHead covers the origin-home case: the
// node already holds the segment in head/ (distribution promoted it there via
// LocalHolder), but the assignment log still lists it because the holder
// receipt has not been recorded yet. Collection must commit the receipt without
// pulling, and must not pull (no source needed for a segment we already hold).
func TestCollectOnceReceiptsSegmentAlreadyInHead(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	segBytes := writeSegmentBytes(t, vaultID, segID, "already there")
	prePath := pullToPreHead(t, root, segID, segBytes)
	if _, _, err := collection.PromoteVerified(prePath, root, 0); err != nil {
		t.Fatal(err)
	}

	pull := newMemoryPull() // intentionally empty: a held segment needs no pull
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     pull,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1 (held segment receipts without pulling)", receipts.count())
	}

	// A second pass must not re-commit: the receipted set dedups until the
	// receipt replicates into the holder set and the log stops assigning it.
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d after second pass, want 1 (idempotent)", receipts.count())
	}
}

// TestCollectOnceReceiptsSegmentPromotedAfterLayoutWarm covers the production
// race: the first collect pass warms the layout cache while head/ is still
// sparse, then distribution promotes a new local segment to head/ before the
// holder receipt is recorded. The next pass must receipt it without pulling.
func TestCollectOnceReceiptsSegmentPromotedAfterLayoutWarm(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segA := glid.New()
	segB := glid.New()
	root := t.TempDir()

	pull := newMemoryPull()
	pull.Put(segA, writeSegmentBytes(t, vaultID, segA, "remote-a"))

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segA,
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     pull,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts after first pass = %d, want 1", receipts.count())
	}

	// Simulate LocalHolder: segment B lands in head/ after the layout cache warmed.
	segBBytes := writeSegmentBytes(t, vaultID, segB, "local-b")
	prePath := pullToPreHead(t, root, segB, segBBytes)
	if _, _, err := collection.PromoteVerified(prePath, root, 0); err != nil {
		t.Fatal(err)
	}

	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segB,
	})
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if receipts.count() != 2 {
		t.Fatalf("receipts after second pass = %d, want 2 (promoted-after-warm segment)", receipts.count())
	}
}

func TestRunCollectsOnNotify(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	pull := newMemoryPull()
	log := &staticLog{}
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     pull,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		pull.Put(segID, writeSegmentBytes(t, vaultID, segID, "async collect"))
		log.setAssigned(collection.AssignedSegment{
			VaultID:   vaultID,
			SegmentID: segID,
		})
		mgr.Notify(vaultID)
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := mgr.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1", receipts.count())
	}
}

func TestRunCollectsOnPublishCompletedSegment(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	now := time.Unix(0, 1_700_000_000_000).UTC()

	fsm := vaultctlfsm.New()
	pull := newMemoryPull()
	pull.Put(segID, writeSegmentBytes(t, vaultID, segID, "fsm collect"))
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     pull,
		Receipts: receipts,
		FSM:      fsm,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		applyPublish(t, fsm, vaultctlfsm.CompletedSegmentEntry{
			SegmentID:     segID,
			RecordCount:   1,
			ByteSize:      64,
			FirstIngestTS: now,
			LastIngestTS:  now,
			Checksum:      9,
			OriginNodeID:  "origin",
			PublishedAt:   now,
		})
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := mgr.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1", receipts.count())
	}
}

func TestRunWithZeroVaults(t *testing.T) {
	t.Parallel()
	mgr := collection.New(collection.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := mgr.Run(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Run() = %v, want context.DeadlineExceeded", err)
	}
}

func applyPublish(t *testing.T, fsm *vaultctlfsm.FSM, entry vaultctlfsm.CompletedSegmentEntry) {
	t.Helper()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(entry)}); err != nil {
		t.Fatalf("apply publish: %v", err)
	}
}

func TestCollectOnceUnknownVault(t *testing.T) {
	t.Parallel()
	mgr := collection.New(collection.Config{})
	err := mgr.CollectOnce(context.Background(), glid.New())
	if err != collection.ErrUnknownVault {
		t.Fatalf("CollectOnce() = %v, want ErrUnknownVault", err)
	}
}

type errPull struct{ err error }

func (p errPull) Pull(context.Context, glid.GLID, glid.GLID, io.Writer) error { return p.err }

// TestCollectOnceWaitersShareWorkerPass verifies concurrent CollectOnce calls
// with an active worker coalesce on one worker pass instead of each caller
// acquiring passMu and running a separate pass.
func TestCollectOnceWaitersShareWorkerPass(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	seg1 := glid.New()
	seg2 := glid.New()
	root := t.TempDir()

	pull := newMemoryPull()
	pull.Put(seg1, writeSegmentBytes(t, vaultID, seg1, "one"))
	pull.Put(seg2, writeSegmentBytes(t, vaultID, seg2, "two"))
	slow := &countingSlowPull{inner: pull, delay: 50 * time.Millisecond}

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: seg1})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: slow, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = mgr.Run(ctx)
	}()
	time.Sleep(150 * time.Millisecond) // initial pass collects seg1

	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: seg2})
	slow.pulls.Store(0)

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = mgr.CollectOnce(context.Background(), vaultID)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("CollectOnce[%d]: %v", i, err)
		}
	}
	if n := slow.pulls.Load(); n != 1 {
		t.Fatalf("pulls during coalesced CollectOnce = %d, want 1", n)
	}
}

// gatedPull blocks the first Pull until release is closed and records which
// segments were pulled, so tests can freeze a collect pass mid-flight.
type gatedPull struct {
	inner   collection.PullClient
	started chan struct{} // closed when the first Pull begins
	release chan struct{} // the first Pull blocks until this is closed
	first   sync.Once
	mu      sync.Mutex
	pulled  map[glid.GLID]bool
}

func (g *gatedPull) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	g.first.Do(func() {
		close(g.started)
		<-g.release
	})
	g.mu.Lock()
	if g.pulled == nil {
		g.pulled = make(map[glid.GLID]bool)
	}
	g.pulled[segmentID] = true
	g.mu.Unlock()
	return g.inner.Pull(ctx, vaultID, segmentID, dest)
}

func (g *gatedPull) pulledSegment(id glid.GLID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pulled[id]
}

// TestCollectOnceWaiterWaitsForFreshPass verifies a CollectOnce arriving while
// a worker pass is already in flight is NOT satisfied by that pass: the pass
// read the log before the request and can return stale success. The waiter
// must be completed by a pass that starts after the request registers
// (gastrolog-38snf4 gate finding — TestCollectOnceWaitersShareWorkerPass flaked
// exactly this way under full-suite load).
func TestCollectOnceWaiterWaitsForFreshPass(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	seg1 := glid.New()
	seg2 := glid.New()
	root := t.TempDir()

	pull := newMemoryPull()
	pull.Put(seg1, writeSegmentBytes(t, vaultID, seg1, "one"))
	pull.Put(seg2, writeSegmentBytes(t, vaultID, seg2, "two"))
	gated := &gatedPull{inner: pull, started: make(chan struct{}), release: make(chan struct{})}

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: seg1})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: gated, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mgr.Run(ctx) }()

	<-gated.started // initial pass is mid-pull on seg1; its log snapshot predates seg2

	log.setAssigned(
		collection.AssignedSegment{VaultID: vaultID, SegmentID: seg1},
		collection.AssignedSegment{VaultID: vaultID, SegmentID: seg2},
	)

	type result struct {
		err        error
		seg2Pulled bool
	}
	done := make(chan result, 1)
	go func() {
		err := mgr.CollectOnce(context.Background(), vaultID)
		// Snapshot immediately on return: the bug is CollectOnce returning
		// BEFORE any pass that could have seen seg2.
		done <- result{err: err, seg2Pulled: gated.pulledSegment(seg2)}
	}()

	// Let the CollectOnce waiter register while the first pass is still frozen.
	time.Sleep(100 * time.Millisecond)
	close(gated.release)

	res := <-done
	if res.err != nil {
		t.Fatalf("CollectOnce: %v", res.err)
	}
	if !res.seg2Pulled {
		t.Fatal("CollectOnce returned before a fresh pass pulled seg2 — waiter was satisfied by the stale in-flight pass")
	}
}

type countingSlowPull struct {
	inner collection.PullClient
	delay time.Duration
	pulls atomic.Int64
}

func (s *countingSlowPull) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	s.pulls.Add(1)
	time.Sleep(s.delay)
	return s.inner.Pull(ctx, vaultID, segmentID, dest)
}

func TestCollectOncePullFailure(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     errPull{err: errors.New("origin unreachable")},
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	err := mgr.CollectOnce(context.Background(), vaultID)
	if err == nil {
		t.Fatal("expected pull error")
	}
	if receipts.count() != 0 {
		t.Fatalf("receipts = %d, want 0 on failed pull", receipts.count())
	}
}

// TestCollectOncePromotesPreHeadOrphan is restart survival for the crash
// window between the pull's rename commit and promote (gastrolog-5zotim): a
// fresh manager finds the assigned segment already sitting in pre-head/. It
// must verify the file against the published checksum, promote it in place —
// no pull; the empty pull client fails any pull attempt — and commit the
// holder receipt. The old behavior skipped the segment forever: the receipt
// never committed and the release gate stalled for its manifest.
func TestCollectOncePromotesPreHeadOrphan(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	data := writeSegmentBytes(t, vaultID, segID, "pre-head orphan")
	pullToPreHead(t, root, segID, data)

	pull := newMemoryPull() // intentionally empty: promote-in-place needs no pull
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
		Checksum:  segmentChecksumOf(t, data),
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: pull, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths.HeadSegment(root, segID)); err != nil {
		t.Fatalf("orphan not promoted to head/: %v", err)
	}
	if _, err := os.Stat(paths.PreHeadSegment(root, segID)); !os.IsNotExist(err) {
		t.Fatal("pre-head orphan should be gone after promote")
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1 (promoted orphan must be receipted)", receipts.count())
	}
}

// TestRunConvergesPreHeadOrphanAtStartup asserts the worker's startup pass —
// with no Notify, no publish event — promotes and receipts a crash-orphaned
// pre-head segment (gastrolog-5zotim restart survival).
func TestRunConvergesPreHeadOrphanAtStartup(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	data := writeSegmentBytes(t, vaultID, segID, "startup orphan")
	pullToPreHead(t, root, segID, data)

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
		Checksum:  segmentChecksumOf(t, data),
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: newMemoryPull(), Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(paths.HeadSegment(root, segID)); err == nil && receipts.count() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("startup pass never converged pre-head orphan: receipts = %d", receipts.count())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCollectOnceRepullsCorruptPreHeadOrphan: a crash-orphaned pre-head file
// that fails verification must be discarded and re-pulled in the same pass —
// pre-head files are never the only copy (the holder keeps completed/ bytes
// until release), so deletion is safe and re-pull restores the segment.
func TestCollectOnceRepullsCorruptPreHeadOrphan(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	if err := paths.EnsurePreHeadDir(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.PreHeadSegment(root, segID), []byte("torn garbage"), 0o600); err != nil {
		t.Fatal(err)
	}

	data := writeSegmentBytes(t, vaultID, segID, "holder copy")
	pull := newMemoryPull()
	pull.Put(segID, data)
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
		Checksum:  segmentChecksumOf(t, data),
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: pull, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(paths.HeadSegment(root, segID))
	if err != nil {
		t.Fatalf("re-pulled segment missing from head/: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("head/ bytes do not match the holder copy after re-pull")
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1", receipts.count())
	}
}

// TestCollectOnceReplacesStaleOrphanWithPulledBytes covers a stale but
// internally-valid pre-head orphan: its own header checksum passes, but it
// does not match the published checksum. The orphan must be discarded and
// the segment re-pulled from the holder in the same pass.
func TestCollectOnceReplacesStaleOrphanWithPulledBytes(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	stale := writeSegmentBytes(t, vaultID, segID, "stale orphan bytes")
	pullToPreHead(t, root, segID, stale)
	published := writeSegmentBytes(t, vaultID, segID, "published bytes")
	// Same-frame-length segments share a record checksum (frame-CRC
	// self-cancellation); the fixtures must differ in length to differ in
	// published checksum.
	if segmentChecksumOf(t, stale) == segmentChecksumOf(t, published) {
		t.Fatal("fixtures must have distinct published checksums")
	}
	pull := newMemoryPull()
	pull.Put(segID, published)

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
		Checksum:  segmentChecksumOf(t, published),
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: pull, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(paths.HeadSegment(root, segID))
	if err != nil {
		t.Fatalf("segment missing from head/: %v", err)
	}
	if !bytes.Equal(got, published) {
		t.Fatal("head/ holds the stale orphan bytes, not the published bytes")
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1", receipts.count())
	}
}

// TestCollectOnceRejectsPulledChecksumMismatch: pulled bytes that are
// internally valid but do not match the published checksum must not be
// promoted or receipted — a holder serving wrong-but-valid bytes would merge
// divergent segments into this home's GLCB (gastrolog-5zotim). The discarded
// pull must converge once the holder serves matching bytes.
func TestCollectOnceRejectsPulledChecksumMismatch(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	data := writeSegmentBytes(t, vaultID, segID, "served bytes")
	pull := newMemoryPull()
	pull.Put(segID, data)
	want := segmentChecksumOf(t, data)
	wrong := want + 1
	if wrong == 0 {
		wrong = 1
	}

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID, Checksum: wrong})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: pull, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	err := mgr.CollectOnce(context.Background(), vaultID)
	if !errors.Is(err, collection.ErrCorruptSegment) {
		t.Fatalf("CollectOnce() = %v, want ErrCorruptSegment", err)
	}
	if _, statErr := os.Stat(paths.HeadSegment(root, segID)); !os.IsNotExist(statErr) {
		t.Fatal("mismatching segment must not reach head/")
	}
	if _, statErr := os.Stat(paths.PreHeadSegment(root, segID)); !os.IsNotExist(statErr) {
		t.Fatal("mismatching pre-head copy must be deleted")
	}
	if receipts.count() != 0 {
		t.Fatalf("receipts = %d, want 0 on checksum mismatch", receipts.count())
	}

	// The published checksum now matches the served bytes: re-pull converges.
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID, Checksum: want})
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(paths.HeadSegment(root, segID)); statErr != nil {
		t.Fatalf("segment missing from head/ after re-pull: %v", statErr)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1 after re-pull", receipts.count())
	}
}

// TestCollectOnceRejectsTamperedHolderBytes: a holder serving a same-length
// content substitution with fixed-up frame CRCs — identical frame geometry,
// different record bytes — must be rejected against the published checksum
// and never promoted or receipted. The content-blind rolling CRC32 passed
// exactly this substitution through segment.Open, publish, and the
// published-checksum verify (gastrolog-1vepg0). Convergence once the holder
// serves authentic bytes is pinned too.
func TestCollectOnceRejectsTamperedHolderBytes(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	data := writeSegmentBytes(t, vaultID, segID, "authentic holder bytes")
	published := segmentChecksumOf(t, data)
	tampered := tamperFrameSameLength(t, data)

	pull := newMemoryPull()
	pull.Put(segID, tampered)
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID, Checksum: published})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: pull, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	err := mgr.CollectOnce(context.Background(), vaultID)
	if !errors.Is(err, collection.ErrCorruptSegment) {
		t.Fatalf("CollectOnce() = %v, want ErrCorruptSegment", err)
	}
	if _, statErr := os.Stat(paths.HeadSegment(root, segID)); !os.IsNotExist(statErr) {
		t.Fatal("tampered segment must not reach head/")
	}
	if _, statErr := os.Stat(paths.PreHeadSegment(root, segID)); !os.IsNotExist(statErr) {
		t.Fatal("tampered pre-head copy must be deleted")
	}
	if receipts.count() != 0 {
		t.Fatalf("receipts = %d, want 0 on tampered holder bytes", receipts.count())
	}

	// The holder recovers and serves the authentic bytes: re-pull converges.
	pull.Put(segID, data)
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(paths.HeadSegment(root, segID)); statErr != nil {
		t.Fatalf("segment missing from head/ after holder recovery: %v", statErr)
	}
	if receipts.count() != 1 {
		t.Fatalf("receipts = %d, want 1 after holder recovery", receipts.count())
	}
}

// mismatchThenMatchPull serves wrong-but-internally-valid bytes for the first
// `bad` pulls, then the published bytes — a holder recovering from serving a
// stale copy.
type mismatchThenMatchPull struct {
	wrong []byte
	right []byte
	bad   atomic.Int32
}

func (p *mismatchThenMatchPull) Pull(_ context.Context, _, _ glid.GLID, dest io.Writer) error {
	data := p.right
	if p.bad.Add(-1) >= 0 {
		data = p.wrong
	}
	_, err := dest.Write(data)
	return err
}

// TestRunRepullsAfterChecksumMismatch pins retryability: a checksum mismatch
// must re-arm the manager's own backoff wake (like any deferred pull) — the
// publish event that assigned the segment already fired, so nothing else
// retries it (gastrolog-5zotim).
func TestRunRepullsAfterChecksumMismatch(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	right := writeSegmentBytes(t, vaultID, segID, "right bytes")
	wrong := writeSegmentBytes(t, vaultID, segID, "stale bytes from before a rewrite")
	// The segment record checksum folds each frame's trailing CRC into the
	// rolling CRC, which cancels the content contribution — segments with
	// identical frame lengths share a checksum. Distinct-length fixtures keep
	// this test meaningful; guard against fixture drift.
	if segmentChecksumOf(t, right) == segmentChecksumOf(t, wrong) {
		t.Fatal("fixtures must have distinct published checksums")
	}
	pull := &mismatchThenMatchPull{wrong: wrong, right: right}
	pull.bad.Store(2)

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
		Checksum:  segmentChecksumOf(t, right),
	})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: pull, Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		if got, err := os.ReadFile(paths.HeadSegment(root, segID)); err == nil && bytes.Equal(got, right) && receipts.count() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("checksum-mismatch pull never retried to convergence: receipts = %d", receipts.count())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCollectSegmentsVerifiesPublishedChecksum: the targeted path (chunking
// build re-fetching manifest refs) reads the published checksum from the
// vault-ctl registry and must reject mismatching bytes too.
func TestCollectSegmentsVerifiesPublishedChecksum(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	now := time.Unix(0, 1_700_000_000_000).UTC()

	data := writeSegmentBytes(t, vaultID, segID, "targeted mismatch")
	want := segmentChecksumOf(t, data)
	wrong := want + 1
	if wrong == 0 {
		wrong = 1
	}
	fsm := vaultctlfsm.New()
	applyPublish(t, fsm, vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      uint64(len(data)),
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      wrong,
		OriginNodeID:  "origin",
		PublishedAt:   now,
	})

	pull := newMemoryPull()
	pull.Put(segID, data)
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: &staticLog{}, Pull: pull, Receipts: receipts, FSM: fsm,
	}); err != nil {
		t.Fatal(err)
	}
	err := mgr.CollectSegments(context.Background(), vaultID, []glid.GLID{segID})
	if !errors.Is(err, collection.ErrCorruptSegment) {
		t.Fatalf("CollectSegments() = %v, want ErrCorruptSegment", err)
	}
	if _, statErr := os.Stat(paths.HeadSegment(root, segID)); !os.IsNotExist(statErr) {
		t.Fatal("mismatching segment must not reach head/")
	}
	if receipts.count() != 0 {
		t.Fatalf("receipts = %d, want 0 on checksum mismatch", receipts.count())
	}
}

func TestUnregisterVaultStopsCollection(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: &staticLog{}, Pull: newMemoryPull(), Receipts: &recordingReceipts{},
	}); err != nil {
		t.Fatal(err)
	}
	mgr.UnregisterVault(vaultID)
	err := mgr.CollectOnce(context.Background(), vaultID)
	if err != collection.ErrUnknownVault {
		t.Fatalf("CollectOnce() = %v, want ErrUnknownVault", err)
	}
}

func TestRunTwiceReturnsErrNotRunning(t *testing.T) {
	t.Parallel()
	mgr := collection.New(collection.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	if err := mgr.Run(ctx); err != collection.ErrNotRunning {
		t.Fatalf("Run() = %v, want ErrNotRunning", err)
	}
	cancel()
	<-done
}

func TestRegisterVaultRequiresDependencies(t *testing.T) {
	t.Parallel()
	mgr := collection.New(collection.Config{})
	err := mgr.RegisterVault(glid.New(), t.TempDir(), collection.VaultConfig{})
	if err == nil {
		t.Fatal("expected error without log/pull/receipts")
	}
}

// retryablePull fails the first `failures` pulls with a retryable error
// (wrapping os.ErrNotExist — the "no holder has bytes yet" catch-up race),
// then delegates to inner.
type retryablePull struct {
	inner    *memoryPull
	failures atomic.Int32
}

func (p *retryablePull) Pull(ctx context.Context, nodeID glid.GLID, segmentID glid.GLID, dest io.Writer) error {
	if p.failures.Add(-1) >= 0 {
		return fmt.Errorf("pull segment %s: %w", segmentID, os.ErrNotExist)
	}
	return p.inner.Pull(ctx, nodeID, segmentID, dest)
}

// TestDeferredPassRetriesWithoutNewEvents pins the retry edge for deferred
// collect passes. A retryable pull failure (catch-up race: registry lists the
// segment before any holder can serve bytes) used to be logged as "deferred"
// and then dropped — the retry relied on a FUTURE publish event, which never
// arrives for the last segments of a burst. The worker must retry on its own
// backoff wake with no external Notify (gastrolog-38snf4: follower homes
// stalled without their sealed-chunk segments, GLCB never materialized).
func TestDeferredPassRetriesWithoutNewEvents(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	segBytes := writeSegmentBytes(t, vaultID, segID, "late-holder")
	inner := newMemoryPull()
	inner.Put(segID, segBytes)
	pull := &retryablePull{inner: inner}
	pull.failures.Store(2)

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID})
	receipts := &recordingReceipts{}

	var passComplete atomic.Int32
	mgr := collection.New(collection.Config{
		OnPassComplete: func(glid.GLID) { passComplete.Add(1) },
	})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     pull,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// The worker's startup pass is the only external trigger; the failed
	// pulls must be retried by the manager itself.
	headPath := paths.HeadSegment(root, segID)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(headPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deferred collect pass never retried: segment missing from head/ with no new publish events")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// head/ appears mid-pass; the receipt batch and OnPassComplete land at
	// pass end — wait rather than asserting instantly.
	for {
		if receipts.count() == 1 && passComplete.Load() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after retry: receipts = %d (want 1), passComplete = %d (want > 0)",
				receipts.count(), passComplete.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPartialPassStillSignalsProgress pins OnPassComplete for passes that
// land some segments while others keep failing retryably. Downstream
// (chunking GLCB build) must be woken for the segments that DID arrive; the
// old code suppressed the signal whenever the pass had any error.
func TestPartialPassStillSignalsProgress(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	okID := glid.New()
	badID := glid.New()
	root := t.TempDir()

	inner := newMemoryPull()
	inner.Put(okID, writeSegmentBytes(t, vaultID, okID, "arrives"))

	log := &staticLog{}
	log.setAssigned(
		collection.AssignedSegment{VaultID: vaultID, SegmentID: okID},
		collection.AssignedSegment{VaultID: vaultID, SegmentID: badID},
	)
	receipts := &recordingReceipts{}

	passComplete := make(chan struct{}, 8)
	mgr := collection.New(collection.Config{
		OnPassComplete: func(glid.GLID) {
			select {
			case passComplete <- struct{}{}:
			default:
			}
		},
	})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     &missingSegmentPull{inner: inner, missing: badID},
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	select {
	case <-passComplete:
	case <-time.After(10 * time.Second):
		t.Fatal("OnPassComplete suppressed for a pass that made partial progress")
	}
	if _, err := os.Stat(paths.HeadSegment(root, okID)); err != nil {
		t.Fatalf("collected segment missing from head/: %v", err)
	}
}

// missingSegmentPull serves inner bytes except for one permanently-missing
// segment, which fails retryably (os.ErrNotExist) on every attempt.
type missingSegmentPull struct {
	inner   *memoryPull
	missing glid.GLID
}

func (p *missingSegmentPull) Pull(ctx context.Context, nodeID glid.GLID, segmentID glid.GLID, dest io.Writer) error {
	if segmentID == p.missing {
		return fmt.Errorf("pull segment %s: %w", segmentID, os.ErrNotExist)
	}
	return p.inner.Pull(ctx, nodeID, segmentID, dest)
}

// failingPull fails every pull retryably (os.ErrNotExist) and closes started
// on the first attempt, so tests can tell the worker's startup pass is running.
type failingPull struct {
	once    sync.Once
	started chan struct{}
}

func (p *failingPull) Pull(_ context.Context, _ glid.GLID, segmentID glid.GLID, _ io.Writer) error {
	p.once.Do(func() { close(p.started) })
	return fmt.Errorf("pull segment %s: %w", segmentID, os.ErrNotExist)
}

// TestPartialPassFiresOnPassCompleteOnce pins the gastrolog-3fv0dt acceptance:
// a pass that lands 2 of 3 segments while the 3rd keeps failing retryably
// fires OnPassComplete exactly once. The joined pull error must not suppress
// the chunking wake for the segments that DID land, and the no-progress
// retry passes that follow must not fire it again.
func TestPartialPassFiresOnPassCompleteOnce(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	ok1 := glid.New()
	ok2 := glid.New()
	bad := glid.New()
	root := t.TempDir()

	inner := newMemoryPull()
	inner.Put(ok1, writeSegmentBytes(t, vaultID, ok1, "lands-1"))
	inner.Put(ok2, writeSegmentBytes(t, vaultID, ok2, "lands-2"))

	log := &staticLog{}
	log.setAssigned(
		collection.AssignedSegment{VaultID: vaultID, SegmentID: ok1},
		collection.AssignedSegment{VaultID: vaultID, SegmentID: ok2},
		collection.AssignedSegment{VaultID: vaultID, SegmentID: bad},
	)
	receipts := &recordingReceipts{}

	var passComplete atomic.Int32
	fired := make(chan struct{}, 1)
	mgr := collection.New(collection.Config{
		OnPassComplete: func(glid.GLID) {
			passComplete.Add(1)
			select {
			case fired <- struct{}{}:
			default:
			}
		},
	})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     &missingSegmentPull{inner: inner, missing: bad},
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// The pass that landed ok1/ok2 must wake chunking despite the joined
	// pull error for bad.
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("OnPassComplete suppressed for a pass that made partial progress")
	}
	// Force one more full pass and wait for its afterCollectPass: ok1/ok2
	// are receipted and present (skip), bad still fails, so the pass makes
	// no progress and must NOT re-fire the wake. CollectOnce completes only
	// after a pass that started after it registered, so the counter check
	// below needs no sleep.
	if err := mgr.CollectOnce(ctx, vaultID); err == nil {
		t.Fatal("follow-up pass reported success while a segment is still missing")
	}
	if got := passComplete.Load(); got != 1 {
		t.Fatalf("OnPassComplete fired %d times, want exactly 1", got)
	}
	for _, id := range []glid.GLID{ok1, ok2} {
		if _, err := os.Stat(paths.HeadSegment(root, id)); err != nil {
			t.Fatalf("collected segment missing from head/: %v", err)
		}
	}
	if receipts.count() != 2 {
		t.Fatalf("receipts = %d, want 2", receipts.count())
	}
}

// TestFullyFailedPassDoesNotFireOnPassComplete pins the zero-progress side of
// gastrolog-3fv0dt: a pass in which every pull fails must not wake chunking —
// OnPassComplete means "something landed in head/", not "a pass ran".
func TestFullyFailedPassDoesNotFireOnPassComplete(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()

	log := &staticLog{}
	log.setAssigned(
		collection.AssignedSegment{VaultID: vaultID, SegmentID: glid.New()},
		collection.AssignedSegment{VaultID: vaultID, SegmentID: glid.New()},
	)
	receipts := &recordingReceipts{}
	pull := &failingPull{started: make(chan struct{})}

	var passComplete atomic.Int32
	mgr := collection.New(collection.Config{
		OnPassComplete: func(glid.GLID) { passComplete.Add(1) },
	})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     pull,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// The first pull attempt proves the worker's startup pass is running, so
	// CollectOnce below takes the worker-pass path (afterCollectPass runs
	// before its waiter completes).
	select {
	case <-pull.started:
	case <-time.After(10 * time.Second):
		t.Fatal("startup collect pass never attempted a pull")
	}
	if err := mgr.CollectOnce(ctx, vaultID); err == nil {
		t.Fatal("fully-failed pass reported success")
	}
	if got := passComplete.Load(); got != 0 {
		t.Fatalf("OnPassComplete fired %d times for zero-progress passes, want 0", got)
	}
	if receipts.count() != 0 {
		t.Fatalf("receipts = %d, want 0", receipts.count())
	}
}

// segGatedPull blocks pulls of one specific segment until release is closed;
// pulls of other segments pass straight through.
type segGatedPull struct {
	inner   collection.PullClient
	gateSeg glid.GLID
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *segGatedPull) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	if segmentID == g.gateSeg {
		g.once.Do(func() { close(g.started) })
		<-g.release
	}
	return g.inner.Pull(ctx, vaultID, segmentID, dest)
}

// TestCollectSegmentsDoesNotWaitForFullPass pins the gastrolog-1b51yf fix:
// a targeted CollectSegments (chunking build materializing manifest refs)
// must complete while a full collect pass is wedged mid-pull. Before the fix
// collectSegments took passMu and the serial seal queue drained at one chunk
// per full pass under backlog.
func TestCollectSegmentsDoesNotWaitForFullPass(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	passSeg := glid.New()
	targetSeg := glid.New()
	root := t.TempDir()

	inner := newMemoryPull()
	inner.Put(passSeg, writeSegmentBytes(t, vaultID, passSeg, "pass-owned"))
	inner.Put(targetSeg, writeSegmentBytes(t, vaultID, targetSeg, "build-needed"))
	// Gate keyed by segment ID: gatedPull's sync.Once gate would block the
	// SECOND Pull inside Once.Do until the first releases, serializing the
	// very concurrency this test asserts.
	gate := &segGatedPull{
		inner:   inner,
		gateSeg: passSeg,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: passSeg})
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     gate,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}

	// Freeze a full pass mid-pull on passSeg.
	passDone := make(chan error, 1)
	go func() { passDone <- mgr.CollectOnce(context.Background(), vaultID) }()
	<-gate.started

	// The targeted pull must not queue behind the frozen pass.
	targetDone := make(chan error, 1)
	go func() {
		targetDone <- mgr.CollectSegments(context.Background(), vaultID, []glid.GLID{targetSeg})
	}()
	select {
	case err := <-targetDone:
		if err != nil {
			t.Fatalf("CollectSegments: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CollectSegments blocked behind the in-flight full pass (gastrolog-1b51yf regression)")
	}
	if _, err := os.Stat(paths.HeadSegment(root, targetSeg)); err != nil {
		t.Fatalf("targeted segment not in head/: %v", err)
	}

	// Unfreeze the pass; it must still finish and land its own segment.
	close(gate.release)
	if err := <-passDone; err != nil {
		t.Fatalf("CollectOnce: %v", err)
	}
	if _, err := os.Stat(paths.HeadSegment(root, passSeg)); err != nil {
		t.Fatalf("pass segment not in head/: %v", err)
	}
}

// concurrencyProbePull blocks each pull briefly and records the peak number
// of pulls in flight, proving the collect pass runs them in parallel.
type concurrencyProbePull struct {
	inner    collection.PullClient
	mu       sync.Mutex
	inflight int
	peak     int
}

func (p *concurrencyProbePull) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	p.mu.Lock()
	p.inflight++
	if p.inflight > p.peak {
		p.peak = p.inflight
	}
	p.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	err := p.inner.Pull(ctx, vaultID, segmentID, dest)
	p.mu.Lock()
	p.inflight--
	p.mu.Unlock()
	return err
}

func (p *concurrencyProbePull) peakInflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// TestCollectOncePullsInParallel: a serial pull loop capped replication at
// ~850 records/s and starved holder-gated chunking behind it; the collect
// pass must run bounded-concurrent pulls while still committing one receipt
// per pulled segment.
func TestCollectOncePullsInParallel(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()

	mem := newMemoryPull()
	probe := &concurrencyProbePull{inner: mem}
	log := &staticLog{}
	const n = 8
	segs := make([]collection.AssignedSegment, 0, n)
	for range n {
		segID := glid.New()
		mem.Put(segID, writeSegmentBytes(t, vaultID, segID, "parallel"))
		segs = append(segs, collection.AssignedSegment{VaultID: vaultID, SegmentID: segID})
	}
	log.setAssigned(segs...)
	receipts := &recordingReceipts{}

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log:      log,
		Pull:     probe,
		Receipts: receipts,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}
	if got := receipts.count(); got != n {
		t.Fatalf("receipts = %d, want %d", got, n)
	}
	if peak := probe.peakInflight(); peak < 2 {
		t.Fatalf("peak in-flight pulls = %d, want >= 2 (pulls ran serially)", peak)
	}
}
