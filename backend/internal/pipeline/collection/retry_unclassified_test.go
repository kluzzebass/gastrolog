package collection_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/waittest"
)

// errPeerUnreachable stands in for a transport failure — the holder's node is
// restarting or the stream broke — which no collection sentinel classifies.
var errPeerUnreachable = errors.New("open pull stream: connection refused")

type unreachableThenServePull struct {
	inner    *memoryPull
	failures atomic.Int32
}

func (p *unreachableThenServePull) Pull(ctx context.Context, nodeID glid.GLID, segmentID glid.GLID, dest io.Writer) error {
	if p.failures.Add(-1) >= 0 {
		return fmt.Errorf("pull segment %s: %w", segmentID, errPeerUnreachable)
	}
	return p.inner.Pull(ctx, nodeID, segmentID, dest)
}

// startCollection runs the manager with one registered vault. The worker's
// startup pass is the only external trigger, so every later pull must come
// from the manager's own retry.
func startCollection(t *testing.T, vaultID glid.GLID, root string, log collection.LogReader, pull collection.PullClient, receipts collection.ReceiptCommitter) {
	t.Helper()
	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{Log: log, Pull: pull, Receipts: receipts}); err != nil {
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
}

// A pull that fails because the holder's node is unreachable leaves the
// segment assigned and uncollected. Nothing else is guaranteed to wake this
// home's collection again — on a quiet vault no further publish arrives — so
// the worker retries the outstanding obligation itself until it lands.
func TestUnreachableHolderPullRetriesWithoutNewEvents(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	inner := newMemoryPull()
	inner.Put(segID, writeSegmentBytes(t, vaultID, segID, "holder restarting"))
	pull := &unreachableThenServePull{inner: inner}
	pull.failures.Store(3)

	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID})
	receipts := &recordingReceipts{}
	startCollection(t, vaultID, root, log, pull, receipts)

	waittest.Progress(t, "unreachable-holder pull retries to completion", func() (string, bool) {
		head, n := headExists(root, segID), receipts.count()
		return fmt.Sprintf("failures left=%d head=%v receipts=%d", pull.failures.Load(), head, n), head && n == 1
	})
}

// One unclassified failure in a pass must not cost the retry of the segments
// that failed alongside it with a known catch-up race.
func TestMixedFailurePassRetriesEverySegment(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	raceID := glid.New()
	downID := glid.New()
	root := t.TempDir()

	inner := newMemoryPull()
	inner.Put(raceID, writeSegmentBytes(t, vaultID, raceID, "catch-up race"))
	inner.Put(downID, writeSegmentBytes(t, vaultID, downID, "holder down"))
	pull := &perSegmentFailPull{inner: inner, fail: map[glid.GLID]*failPlan{
		raceID: {left: 2, err: collection.ErrSegmentUnavailable},
		downID: {left: 2, err: errPeerUnreachable},
	}}

	log := &staticLog{}
	log.setAssigned(
		collection.AssignedSegment{VaultID: vaultID, SegmentID: raceID},
		collection.AssignedSegment{VaultID: vaultID, SegmentID: downID},
	)
	receipts := &recordingReceipts{}
	startCollection(t, vaultID, root, log, pull, receipts)

	waittest.Progress(t, "mixed-failure pass retries both segments", func() (string, bool) {
		race, down, n := headExists(root, raceID), headExists(root, downID), receipts.count()
		return fmt.Sprintf("%s race=%v down=%v receipts=%d", pull.state(), race, down, n), race && down && n == 2
	})
}

type failPlan struct {
	left int
	err  error
}

type perSegmentFailPull struct {
	inner *memoryPull
	mu    sync.Mutex
	fail  map[glid.GLID]*failPlan
}

func (p *perSegmentFailPull) Pull(ctx context.Context, nodeID glid.GLID, segmentID glid.GLID, dest io.Writer) error {
	p.mu.Lock()
	plan := p.fail[segmentID]
	if plan != nil && plan.left > 0 {
		plan.left--
		err := plan.err
		p.mu.Unlock()
		return fmt.Errorf("pull segment %s: %w", segmentID, err)
	}
	p.mu.Unlock()
	return p.inner.Pull(ctx, nodeID, segmentID, dest)
}

func (p *perSegmentFailPull) state() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var b []byte
	for id, plan := range p.fail {
		b = fmt.Appendf(b, "%s:left=%d ", id.String()[:6], plan.left)
	}
	return string(b)
}
