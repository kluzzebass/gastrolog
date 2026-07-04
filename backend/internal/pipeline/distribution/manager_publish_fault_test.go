package distribution

// Regression coverage for gastrolog-353kwm: a missing-bytes item must not
// abort its coalesced publish batch (stranding durable batchmates until
// restart), and failed vault-ctl applies must retry behind a backoff instead
// of hot-looping invisibly.

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
)

// TestPublishBatchSkipsMissingBytesItem: one of three coalesced items lost its
// on-disk bytes (purge raced the queue). Only that item is forgotten; the
// surviving batchmates publish. Before the fix the whole batch returned
// errPublishBytesMissing — non-retryable — and the survivors stayed in
// v.segments where the stranded rescan skips them as known: durable segments
// invisible to vault-ctl until process restart.
func TestPublishBatchSkipsMissingBytesItem(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &countPublisher{}

	mgr, _ := New(Config{})
	if err := mgr.RegisterVault(vaultID, root, VaultConfig{Publisher: pub}); err != nil {
		t.Fatal(err)
	}
	mgr.mu.Lock()
	v := mgr.vaults[vaultID]
	mgr.mu.Unlock()

	segIDs := []glid.GLID{glid.New(), glid.New(), glid.New()}
	items := make([]pendingPublish, len(segIDs))
	for i, segID := range segIDs {
		_, meta := writeCompleted(t, root, vaultID, segID)
		items[i] = pendingPublish{
			vaultID: vaultID,
			segID:   segID,
			path:    paths.CompletedSegment(root, segID),
			meta:    meta,
		}
	}
	// Purge the middle item's bytes, as ReleaseSegments would.
	if err := os.Remove(items[1].path); err != nil {
		t.Fatal(err)
	}

	if err := v.publishStagedBatch(context.Background(), items); err != nil {
		t.Fatalf("publishStagedBatch = %v, want nil (missing-bytes item skipped individually)", err)
	}
	if pub.n != 2 {
		t.Fatalf("published %d segments, want 2 (batchmates of the purged item)", pub.n)
	}
	if !v.isRetired(segIDs[1]) {
		t.Fatal("purged item should be forgotten (retired)")
	}
	for _, i := range []int{0, 2} {
		if v.isRetired(segIDs[i]) {
			t.Fatalf("surviving batchmate %d wrongly retired", i)
		}
	}
}

// failNThenOKPublisher fails the first n Publish calls, then succeeds.
type failNThenOKPublisher struct {
	failures  int32
	attempts  atomic.Int32
	published atomic.Int32
}

func (p *failNThenOKPublisher) Publish(context.Context, Metadata) error {
	if p.attempts.Add(1) <= p.failures {
		return errors.New("vault-ctl apply failed (injected)")
	}
	p.published.Add(1)
	return nil
}

// TestPublishRetryBackoffEventuallyPublishes: transient vault-ctl apply
// failures retry behind the backoff wake and eventually commit. The attempt
// count stays bounded — the pre-fix retry path re-notified itself with no
// delay, spinning thousands of attempts per second while applies failed.
func TestPublishRetryBackoffEventuallyPublishes(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &failNThenOKPublisher{failures: 3}

	mgr, _ := New(Config{})
	if err := mgr.RegisterVault(vaultID, root, VaultConfig{Publisher: pub}); err != nil {
		t.Fatal(err)
	}

	completed := make(chan segmentation.CompletedSegment, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Run(ctx, completed)
	}()
	defer func() {
		cancel()
		<-done
	}()

	seg, _ := writeCompleted(t, root, vaultID, glid.New())
	completed <- seg

	deadline := time.Now().Add(10 * time.Second)
	for pub.published.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("segment never published; attempts=%d", pub.attempts.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 3 failures + 1 success, plus slack for an external wake or two. A hot
	// loop racks up orders of magnitude more before the backoff window ends.
	if n := pub.attempts.Load(); n > 10 {
		t.Fatalf("publish attempts = %d, want <=10 (retry loop must back off)", n)
	}
}
