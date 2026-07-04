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
	prePath, err := collection.ReceiveToPreHead(root, segID, bytes.NewReader(segBytes))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.PromoteVerified(prePath, root); err != nil {
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
	prePath, err := collection.ReceiveToPreHead(root, segB, bytes.NewReader(segBBytes))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.PromoteVerified(prePath, root); err != nil {
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

func TestCollectOnceSkipsSegmentInPreHead(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	data := writeSegmentBytes(t, vaultID, segID, "pre-head only")
	if _, err := collection.ReceiveToPreHead(root, segID, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	pull := newMemoryPull()
	pull.Put(segID, data)
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID})
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
	if receipts.count() != 0 {
		t.Fatalf("receipts = %d, want 0 while segment still in pre-head", receipts.count())
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
