package distribution

import (
	"context"
	"os"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

type countPublisher struct {
	n int
}

func (p *countPublisher) Publish(context.Context, Metadata) error {
	p.n++
	return nil
}

func writeCompleted(t *testing.T, root string, vaultID, segID glid.GLID) (segmentation.CompletedSegment, Metadata) {
	t.Helper()
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		t.Fatal(err)
	}
	path := paths.CompletedSegment(root, segID)
	sf, err := segment.Create(path, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC)
	rec := &record.Record{
		SourceTS: ts,
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   ts,
		},
		Raw: []byte("payload"),
	}
	if err := sf.Append(rec, ts); err != nil {
		t.Fatal(err)
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	hdr := sf.Header()
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
	seg := segmentation.CompletedSegment{
		VaultID:   vaultID,
		SegmentID: segID,
		Path:      path,
		Header:    hdr,
	}
	meta, err := metadataForPublish(seg)
	if err != nil {
		t.Fatal(err)
	}
	return seg, meta
}

func TestPublishStagedNoOpAfterRetireSegments(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()
	pub := &countPublisher{}

	mgr, _ := New(Config{})
	if err := mgr.RegisterVault(vaultID, root, VaultConfig{Publisher: pub}); err != nil {
		t.Fatal(err)
	}

	_, meta := writeCompleted(t, root, vaultID, segID)
	mgr.mu.Lock()
	v := mgr.vaults[vaultID]
	mgr.mu.Unlock()

	if err := v.publishStaged(context.Background(), meta, segID, paths.CompletedSegment(root, segID)); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if pub.n != 1 {
		t.Fatalf("publish count = %d, want 1", pub.n)
	}

	mgr.RetireSegments(vaultID, []glid.GLID{segID})
	if err := os.Remove(paths.CompletedSegment(root, segID)); err != nil {
		t.Fatal(err)
	}

	// Stale queue item or publish retry after ReleaseSegments must not
	// re-commit vault-ctl metadata without bytes on disk.
	if err := v.publishStaged(context.Background(), meta, segID, paths.CompletedSegment(root, segID)); err != nil {
		t.Fatalf("stale publish: %v", err)
	}
	if pub.n != 1 {
		t.Fatalf("publish count after retire = %d, want 1 (no ghost registry entry)", pub.n)
	}
	if !v.isRetired(segID) {
		t.Fatal("segment should be retired after stale publish with missing bytes")
	}
}

func TestPublishStagedNoOpWhenRetiredDuringPublish(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	mgr, _ := New(Config{})
	if err := mgr.RegisterVault(vaultID, root, VaultConfig{
		Publisher:   &countPublisher{},
		LocalHolder: func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	mgr.mu.Lock()
	v := mgr.vaults[vaultID]
	mgr.mu.Unlock()

	_, meta := writeCompleted(t, root, vaultID, segID)
	path := paths.CompletedSegment(root, segID)

	// Retire between Publish and finalizeAfterPublish — must not promote to head.
	racePub := &countPublisher{}
	orig := v.publisher
	v.publisher = &retireOnPublish{inner: racePub, retire: func() {
		mgr.RetireSegments(vaultID, []glid.GLID{segID})
	}}
	defer func() { v.publisher = orig }()

	if err := v.publishStaged(context.Background(), meta, segID, path); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if racePub.n != 1 {
		t.Fatalf("publish count = %d, want 1", racePub.n)
	}
	if _, err := os.Stat(paths.HeadSegment(root, segID)); err == nil {
		t.Fatal("retired segment must not promote to head after publish race")
	}
}

type retireOnPublish struct {
	inner  *countPublisher
	retire func()
}

func (p *retireOnPublish) Publish(ctx context.Context, meta Metadata) error {
	if err := p.inner.Publish(ctx, meta); err != nil {
		return err
	}
	if p.retire != nil {
		p.retire()
	}
	return nil
}

// TestRetireSegmentsMatchesRetireSegment asserts the manager's batch
// RetireSegments entry point and vaultDist.retireSegment's direct per-ID entry
// point (the one publishStaged/publishStagedBatch use for the bytes-missing
// path) leave identical bookkeeping: dropped from v.segments, added to
// v.retired. RetireSegments is a thin loop over retireSegment
// (gastrolog-2zvfp2) — this pins that the two entry points cannot diverge.
func TestRetireSegmentsMatchesRetireSegment(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()

	mgr, _ := New(Config{})
	if err := mgr.RegisterVault(vaultID, root, VaultConfig{Publisher: &countPublisher{}}); err != nil {
		t.Fatal(err)
	}
	mgr.mu.Lock()
	v := mgr.vaults[vaultID]
	mgr.mu.Unlock()

	batchA, batchB := glid.New(), glid.New()
	direct := glid.New()

	// Seed all three as known (as prepare() would have during staging).
	v.mu.Lock()
	v.segments[batchA] = paths.CompletedSegment(root, batchA)
	v.segments[batchB] = paths.CompletedSegment(root, batchB)
	v.segments[direct] = paths.CompletedSegment(root, direct)
	v.mu.Unlock()

	// Retire two through the manager's batch entry point...
	mgr.RetireSegments(vaultID, []glid.GLID{batchA, batchB})
	// ...and one directly through vaultDist.retireSegment, as the
	// bytes-missing path in publishStaged/publishStagedBatch does.
	v.retireSegment(direct)

	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, id := range []glid.GLID{batchA, batchB, direct} {
		if _, known := v.segments[id]; known {
			t.Errorf("segment %s still in v.segments after retirement", id)
		}
		if _, retired := v.retired[id]; !retired {
			t.Errorf("segment %s missing from v.retired after retirement", id)
		}
	}
}

func TestSegmentPathForPullMissingFile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	v, err := newVaultDist(root, VaultConfig{
		Publisher: &countPublisher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	v.segments[segID] = paths.CompletedSegment(root, segID)
	v.mu.Unlock()

	if path, ok := v.segmentPathForPull(segID); ok {
		t.Fatalf("segmentPathForPull() = (%q, true), want false for missing file", path)
	}
}
