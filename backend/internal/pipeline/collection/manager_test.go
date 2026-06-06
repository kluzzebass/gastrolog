package collection_test

import (
	"bytes"
	"context"
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

func TestCollectOnceSkipsSegmentAlreadyInHead(t *testing.T) {
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

	pull := newMemoryPull()
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
	if receipts.count() != 0 {
		t.Fatalf("receipts = %d, want 0", receipts.count())
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
