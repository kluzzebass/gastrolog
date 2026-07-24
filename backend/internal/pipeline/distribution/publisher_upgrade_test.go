package distribution_test

// Coverage for gastrolog-375el: a vault registered while no vault-ctl handle
// exists runs a fail-closed publisher, and the publisher upgrade
// (re-registration with the real publisher) must republish every completed
// segment the no-handle window refused — without a restart and without a
// completed-channel overflow. The recovery event is the registration itself:
// distribution.Manager.RegisterVault wakes the stranded rescan and the retry
// drain, and the per-segment single-flight in vaultDist keeps the overlapping
// recovery paths (publish queue, retry queue, rescan) from double-publishing.

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
)

// upgradeWait bounds a single awaited event in these tests. The waits are
// event-driven (promotion/attempt channels), never sampled; the bound only
// turns a wedged pipeline into a test failure instead of a package timeout.
const upgradeWait = 30 * time.Second

// refusingPublisher rejects every publish with a retryable error — the shape
// of the orchestrator's no-handle publisher — and signals each attempt.
type refusingPublisher struct {
	attempts chan struct{}
}

func (p *refusingPublisher) Publish(context.Context, distribution.Metadata) error {
	select {
	case p.attempts <- struct{}{}:
	default:
	}
	return errors.New("no vault-ctl handle; segment publish deferred")
}

// switchPublisher records successful publishes and fails while closed. It
// stands in for the real VaultCtlPublisher across a leadership outage.
type switchPublisher struct {
	mu        sync.Mutex
	open      bool
	published []glid.GLID
	attempts  chan struct{}
}

func (p *switchPublisher) Publish(_ context.Context, meta distribution.Metadata) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.attempts != nil {
		select {
		case p.attempts <- struct{}{}:
		default:
		}
	}
	if !p.open {
		return errors.New("vault-ctl apply failed: no leader")
	}
	p.published = append(p.published, meta.SegmentID)
	return nil
}

func (p *switchPublisher) setOpen(open bool) {
	p.mu.Lock()
	p.open = open
	p.mu.Unlock()
}

func (p *switchPublisher) publishes(segID glid.GLID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, id := range p.published {
		if id == segID {
			n++
		}
	}
	return n
}

// runManager starts a distribution manager and returns it with its completed
// channel; the manager stops on test cleanup.
func runManager(t *testing.T) (*distribution.Manager, chan segmentation.CompletedSegment) {
	t.Helper()
	mgr, _ := distribution.New(distribution.Config{})
	completed := make(chan segmentation.CompletedSegment, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Run(ctx, completed)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return mgr, completed
}

func waitEvent(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(upgradeWait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitPromoted(t *testing.T, ch <-chan glid.GLID, what string) glid.GLID {
	t.Helper()
	select {
	case id := <-ch:
		return id
	case <-time.After(upgradeWait):
		t.Fatalf("timed out waiting for %s", what)
		return glid.Nil
	}
}

// TestPublisherUpgradeRepublishesRefusedSegments is the core gastrolog-375el
// scenario: a segment completed while the vault's publisher refuses (no
// vault-ctl handle) becomes visible — vault-ctl committed and head-promoted —
// purely from the publisher-upgrade re-registration. No restart, no second
// completed-channel delivery, no overflow-driven rescan.
func TestPublisherUpgradeRepublishesRefusedSegments(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	mgr, completed := runManager(t)

	refusing := &refusingPublisher{attempts: make(chan struct{}, 1)}
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher:   refusing,
		LocalHolder: func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}

	seg := writeCompletedSegment(t, root, vaultID, "refused-then-published")
	completed <- seg
	waitEvent(t, refusing.attempts, "fail-closed publish attempt")

	if got := mgr.PublishStats(); len(got) != 1 || got[0].Published != 0 {
		t.Fatalf("PublishStats during no-handle window = %+v, want vault at 0", got)
	}

	// Publisher upgrade: the orchestrator re-registers the vault once the
	// vault-ctl handle appears (fresh vaultDist, real publisher, local holder).
	upgraded := &switchPublisher{open: true}
	promoted := make(chan glid.GLID, 4)
	mgr.UnregisterVault(vaultID)
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher:           upgraded,
		LocalHolder:         func() bool { return true },
		OnLocalHeadPromoted: func(id glid.GLID) { promoted <- id },
	}); err != nil {
		t.Fatal(err)
	}

	if id := waitPromoted(t, promoted, "head promotion after publisher upgrade"); id != seg.SegmentID {
		t.Fatalf("promoted segment = %s, want %s", id, seg.SegmentID)
	}
	headPath := paths.HeadSegment(root, seg.SegmentID)
	if !fileExists(t, headPath) {
		t.Fatalf("head copy missing after upgrade republish: %s", headPath)
	}
	if n := upgraded.publishes(seg.SegmentID); n != 1 {
		t.Fatalf("segment published %d times after upgrade, want exactly 1", n)
	}
	if got := mgr.PublishStats(); len(got) != 1 || got[0].Published != 1 {
		t.Fatalf("PublishStats after upgrade = %+v, want vault at 1", got)
	}

	// Idempotency: a direct re-publish of the same completed segment (the
	// sync path any late duplicate would take) is a no-op — the segment is
	// already fully published.
	if err := mgr.PublishCompleted(context.Background(), seg); err != nil {
		t.Fatalf("duplicate PublishCompleted = %v, want nil", err)
	}
	if n := upgraded.publishes(seg.SegmentID); n != 1 {
		t.Fatalf("segment published %d times after duplicate, want exactly 1", n)
	}
	if got := mgr.PublishStats(); got[0].Published != 1 {
		t.Fatalf("PublishStats after duplicate = %+v, want vault at 1", got)
	}
}

// TestRegisterVaultAfterRunPublishesExistingBacklog: a vault registered while
// the manager is already running (restart backlog, placement arriving late)
// publishes everything already in completed/ from the registration wake alone
// — no completed-channel delivery ever happens for these segments.
func TestRegisterVaultAfterRunPublishesExistingBacklog(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	mgr, _ := runManager(t)

	segA := writeCompletedSegment(t, root, vaultID, "backlog-a")
	segB := writeCompletedSegment(t, root, vaultID, "backlog-b")

	pub := &switchPublisher{open: true}
	promoted := make(chan glid.GLID, 4)
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher:           pub,
		LocalHolder:         func() bool { return true },
		OnLocalHeadPromoted: func(id glid.GLID) { promoted <- id },
	}); err != nil {
		t.Fatal(err)
	}

	got := map[glid.GLID]bool{}
	got[waitPromoted(t, promoted, "first backlog promotion")] = true
	got[waitPromoted(t, promoted, "second backlog promotion")] = true
	if !got[segA.SegmentID] || !got[segB.SegmentID] {
		t.Fatalf("promoted %v, want both %s and %s", got, segA.SegmentID, segB.SegmentID)
	}
	for _, seg := range []segmentation.CompletedSegment{segA, segB} {
		if n := pub.publishes(seg.SegmentID); n != 1 {
			t.Fatalf("segment %s published %d times, want exactly 1", seg.SegmentID, n)
		}
	}
	if stats := mgr.PublishStats(); len(stats) != 1 || stats[0].Published != 2 {
		t.Fatalf("PublishStats = %+v, want vault at 2", stats)
	}
}

// TestPublisherUpgradeIntoFailingPublisherRecovers: the upgrade itself can
// land during a vault-ctl leadership outage — the re-registered publisher
// also fails at first. The refused segments must survive in the retry queue
// of the NEW registration and publish when the outage ends
// (NotifyPublishRetry, the leadership-gain wake).
func TestPublisherUpgradeIntoFailingPublisherRecovers(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	mgr, completed := runManager(t)

	refusing := &refusingPublisher{attempts: make(chan struct{}, 1)}
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher: refusing,
	}); err != nil {
		t.Fatal(err)
	}
	seg := writeCompletedSegment(t, root, vaultID, "upgrade-into-outage")
	completed <- seg
	waitEvent(t, refusing.attempts, "fail-closed publish attempt")

	// Upgrade while the real publisher is still failing (leaderless window).
	upgraded := &switchPublisher{open: false, attempts: make(chan struct{}, 1)}
	promoted := make(chan glid.GLID, 4)
	mgr.UnregisterVault(vaultID)
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher:           upgraded,
		LocalHolder:         func() bool { return true },
		OnLocalHeadPromoted: func(id glid.GLID) { promoted <- id },
	}); err != nil {
		t.Fatal(err)
	}
	// The registration wake re-attempts through the new publisher and fails —
	// proof the segment reached the new registration's retry path.
	waitEvent(t, upgraded.attempts, "failing attempt on upgraded publisher")

	// Outage ends: leadership-gain wake drains the retries.
	upgraded.setOpen(true)
	mgr.NotifyPublishRetry()

	if id := waitPromoted(t, promoted, "promotion after outage recovery"); id != seg.SegmentID {
		t.Fatalf("promoted segment = %s, want %s", id, seg.SegmentID)
	}
	if n := upgraded.publishes(seg.SegmentID); n != 1 {
		t.Fatalf("segment published %d times, want exactly 1", n)
	}
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}
