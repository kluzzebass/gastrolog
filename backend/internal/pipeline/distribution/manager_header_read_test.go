package distribution_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
)

// TestPublishIngressBacklogReadsHeadersOnly is the gastrolog-faj2yv acceptance
// test: a restart backlog of completed/ segments must publish without a single
// full-verify segment.Open — the stranded rescan and publish staging read only
// the fixed header of each file.
func TestPublishIngressBacklogReadsHeadersOnly(t *testing.T) {
	// Not parallel: asserts on the segment package's process-wide counters.
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}

	const backlog = 5
	want := make(map[glid.GLID]uint64, backlog) // segment ID → header checksum
	for range backlog {
		seg := writeCompletedSegment(t, root, vaultID, "restart-backlog")
		want[seg.SegmentID] = seg.Header.SegmentChecksum
	}

	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher: pub,
	}); err != nil {
		t.Fatal(err)
	}

	opensBefore := segment.Opens()
	mappedBefore := segment.MappedOpens()

	completed := make(chan segmentation.CompletedSegment)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, completed)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	deadline := time.Now().Add(10 * time.Second)
	for pub.count() < backlog {
		if time.Now().After(deadline) {
			t.Fatalf("rescan published %d of %d backlog segments", pub.count(), backlog)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if d := segment.Opens() - opensBefore; d != 0 {
		t.Errorf("backlog publish performed %d full-verify segment.Open calls, want 0", d)
	}
	if d := segment.MappedOpens() - mappedBefore; d != 0 {
		t.Errorf("backlog publish performed %d mapped opens, want 0", d)
	}

	// The header-only reads must still carry real metadata into the publish.
	pub.mu.Lock()
	published := append([]distribution.Metadata(nil), pub.published...)
	pub.mu.Unlock()
	for _, meta := range published {
		checksum, ok := want[meta.SegmentID]
		if !ok {
			t.Errorf("published unknown segment %s", meta.SegmentID)
			continue
		}
		if meta.RecordCount != 1 || meta.Checksum != checksum || meta.ByteSize == 0 {
			t.Errorf("published meta = %+v, want RecordCount 1, checksum %d", meta, checksum)
		}
	}
}

// TestPublishCompletedZeroHeaderReadsHeaderOnly covers the metadataForPublish
// fallback: a completed segment delivered without a decoded header must fill
// metadata from a fixed-header read, not a full-verify Open (gastrolog-faj2yv).
func TestPublishCompletedZeroHeaderReadsHeaderOnly(t *testing.T) {
	// Not parallel: asserts on the segment package's process-wide counters.
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}

	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher: pub,
	}); err != nil {
		t.Fatal(err)
	}

	seg := writeCompletedSegment(t, root, vaultID, "zero-header")
	wantChecksum := seg.Header.SegmentChecksum
	seg.Header = segment.Header{} // simulate a delivery that lost the header

	opensBefore := segment.Opens()
	headerReadsBefore := segment.HeaderReads()
	if err := mgr.PublishCompleted(context.Background(), seg); err != nil {
		t.Fatal(err)
	}

	if d := segment.Opens() - opensBefore; d != 0 {
		t.Errorf("zero-header publish performed %d full-verify segment.Open calls, want 0", d)
	}
	if d := segment.HeaderReads() - headerReadsBefore; d != 1 {
		t.Errorf("zero-header publish performed %d header reads, want 1", d)
	}
	meta := pub.last()
	if meta.SegmentID != seg.SegmentID || meta.RecordCount != 1 || meta.Checksum != wantChecksum {
		t.Fatalf("published meta = %+v, want checksum %d", meta, wantChecksum)
	}
}
