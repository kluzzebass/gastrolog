package collection_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
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

func (r *recordingReceipts) CommitHolderReceipt(_ context.Context, _ glid.GLID, segmentID glid.GLID) error {
	r.mu.Lock()
	r.receipts = append(r.receipts, segmentID)
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
