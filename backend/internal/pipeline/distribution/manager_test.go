package distribution_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/record"
)

type recordingPublisher struct {
	mu        sync.Mutex
	published []distribution.Metadata
}

func (p *recordingPublisher) Publish(_ context.Context, meta distribution.Metadata) error {
	p.mu.Lock()
	p.published = append(p.published, meta)
	p.mu.Unlock()
	return nil
}

func (p *recordingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

func (p *recordingPublisher) last() distribution.Metadata {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.published) == 0 {
		return distribution.Metadata{}
	}
	return p.published[len(p.published)-1]
}

func writeCompletedSegment(t *testing.T, vaultRoot string, vaultID glid.GLID, raw string) segmentation.CompletedSegment {
	t.Helper()
	if err := paths.EnsureSegmentationDirs(vaultRoot); err != nil {
		t.Fatal(err)
	}
	segID := glid.New()
	path := paths.CompletedSegment(vaultRoot, segID)

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
			IngestSeq:  0,
		},
		Attrs: record.Attributes{"k": "v"},
		Raw:   []byte(raw),
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
	return segmentation.CompletedSegment{
		VaultID: vaultID,
		Meta:    segment.Meta{ID: segID, VaultID: vaultID},
		Path:    path,
		Header:  hdr,
	}
}

func TestMetadataFromCompletedSegment(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	seg := writeCompletedSegment(t, root, vaultID, "payload")

	meta, err := distribution.MetadataFrom(seg)
	if err != nil {
		t.Fatal(err)
	}
	if meta.SegmentID != seg.Meta.ID || meta.VaultID != vaultID {
		t.Fatalf("meta = %+v", meta)
	}
	if meta.RecordCount != 1 || meta.Checksum != seg.Header.SegmentChecksum {
		t.Fatalf("meta = %+v", meta)
	}
	if meta.ByteSize == 0 {
		t.Error("expected non-zero byte size")
	}
}

func TestPromoteToHead(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	vaultID := glid.New()
	seg := writeCompletedSegment(t, root, vaultID, "move me")

	dest, err := distribution.PromoteToHead(seg.Path, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("head file: %v", err)
	}
	if _, err := os.Stat(seg.Path); !os.IsNotExist(err) {
		t.Fatal("completed copy should be gone after promote")
	}
}

func TestPublishOnCompleted(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}

	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher:   pub,
		LocalHolder: func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}

	seg := writeCompletedSegment(t, root, vaultID, "published")
	if err := mgr.PublishCompleted(context.Background(), seg); err != nil {
		t.Fatal(err)
	}

	meta := pub.last()
	if meta.SegmentID != seg.Meta.ID || meta.RecordCount != 1 {
		t.Fatalf("published = %+v", meta)
	}
	if _, err := os.Stat(seg.Path); err != nil {
		t.Fatal("remote holder keeps segment in completed/")
	}
}

func TestLocalHolderPromotesToHead(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}

	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher:   pub,
		LocalHolder: func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}

	seg := writeCompletedSegment(t, root, vaultID, "local")
	if err := mgr.PublishCompleted(context.Background(), seg); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(seg.Path); !os.IsNotExist(err) {
		t.Fatal("completed path should be empty after local promote")
	}
	headPath := paths.HeadSegment(root, seg.Meta.ID)
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head file: %v", err)
	}
}

func TestServePullStreamsBytes(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}

	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher: pub,
	}); err != nil {
		t.Fatal(err)
	}

	seg := writeCompletedSegment(t, root, vaultID, "pull-bytes")
	if err := mgr.PublishCompleted(context.Background(), seg); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := mgr.ServePull(distribution.PullRequest{
		VaultID:   vaultID,
		SegmentID: seg.Meta.ID,
		Dest:      &buf,
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("pull-bytes")) {
		t.Fatalf("got %q", buf.Bytes())
	}
}

func TestServePullNotFound(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, t.TempDir(), distribution.VaultConfig{
		Publisher: &recordingPublisher{},
	}); err != nil {
		t.Fatal(err)
	}
	err := mgr.ServePull(distribution.PullRequest{
		VaultID:   vaultID,
		SegmentID: glid.New(),
		Dest:      &bytes.Buffer{},
	})
	if err != distribution.ErrSegmentNotFound {
		t.Fatalf("ServePull() = %v, want ErrSegmentNotFound", err)
	}
}

func TestRunConsumesCompletedChannel(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}

	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher: pub,
	}); err != nil {
		t.Fatal(err)
	}

	completed := make(chan segmentation.CompletedSegment, 1)
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

	completed <- writeCompletedSegment(t, root, vaultID, "async")
	time.Sleep(50 * time.Millisecond)

	if pub.count() != 1 {
		t.Fatalf("published %d metadata entries", pub.count())
	}
}

type errPublisher struct{ err error }

func (p errPublisher) Publish(context.Context, distribution.Metadata) error { return p.err }

func TestPublishCompletedRollsBackOnPublisherError(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
		Publisher: errPublisher{err: errors.New("raft unavailable")},
	}); err != nil {
		t.Fatal(err)
	}
	seg := writeCompletedSegment(t, root, vaultID, "rollback")
	err := mgr.PublishCompleted(context.Background(), seg)
	if err == nil {
		t.Fatal("expected publish error")
	}
	err = mgr.ServePull(distribution.PullRequest{
		VaultID: vaultID, SegmentID: seg.Meta.ID, Dest: &bytes.Buffer{},
	})
	if !errors.Is(err, distribution.ErrSegmentNotFound) {
		t.Fatalf("ServePull after failed publish = %v, want ErrSegmentNotFound", err)
	}
}

func TestPublishCompletedUnknownVault(t *testing.T) {
	t.Parallel()
	mgr, _ := distribution.New(distribution.Config{})
	seg := writeCompletedSegment(t, t.TempDir(), glid.New(), "orphan")
	err := mgr.PublishCompleted(context.Background(), seg)
	if !errors.Is(err, distribution.ErrUnknownVault) {
		t.Fatalf("PublishCompleted() = %v, want ErrUnknownVault", err)
	}
}

func TestMetadataFromMissingFile(t *testing.T) {
	t.Parallel()
	seg := segmentation.CompletedSegment{
		VaultID: glid.New(),
		Meta:    segment.Meta{ID: glid.New(), VaultID: glid.New()},
		Path:    filepath.Join(t.TempDir(), "missing"),
	}
	if _, err := distribution.MetadataFrom(seg); err == nil {
		t.Fatal("expected stat error")
	}
}

func TestRegisterVaultRequiresPublisher(t *testing.T) {
	t.Parallel()
	mgr, _ := distribution.New(distribution.Config{})
	err := mgr.RegisterVault(glid.New(), t.TempDir(), distribution.VaultConfig{})
	if err == nil {
		t.Fatal("expected error without publisher")
	}
}

func TestRegisterVaultDuplicate(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	mgr, _ := distribution.New(distribution.Config{})
	cfg := distribution.VaultConfig{Publisher: &recordingPublisher{}}
	if err := mgr.RegisterVault(vaultID, t.TempDir(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := mgr.RegisterVault(vaultID, t.TempDir(), cfg); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

func TestUnregisterVaultStopsServingPull(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}
	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{Publisher: pub}); err != nil {
		t.Fatal(err)
	}
	seg := writeCompletedSegment(t, root, vaultID, "gone")
	if err := mgr.PublishCompleted(context.Background(), seg); err != nil {
		t.Fatal(err)
	}
	mgr.UnregisterVault(vaultID)
	err := mgr.ServePull(distribution.PullRequest{
		VaultID: vaultID, SegmentID: seg.Meta.ID, Dest: &bytes.Buffer{},
	})
	if !errors.Is(err, distribution.ErrUnknownVault) {
		t.Fatalf("ServePull() = %v, want ErrUnknownVault", err)
	}
}

func TestRunTwiceReturnsErrNotRunning(t *testing.T) {
	t.Parallel()
	mgr, _ := distribution.New(distribution.Config{})
	completed := make(chan segmentation.CompletedSegment)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, completed)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	if err := mgr.Run(ctx, completed); !errors.Is(err, distribution.ErrNotRunning) {
		t.Fatalf("Run() = %v, want ErrNotRunning", err)
	}
	cancel()
	<-done
}

func TestRunPullViaChannel(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}
	mgr, pullIn := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{Publisher: pub}); err != nil {
		t.Fatal(err)
	}
	completed := make(chan segmentation.CompletedSegment, 1)
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

	seg := writeCompletedSegment(t, root, vaultID, "async-pull")
	completed <- seg
	time.Sleep(50 * time.Millisecond)

	var buf bytes.Buffer
	pullIn <- distribution.PullRequest{
		VaultID: vaultID, SegmentID: seg.Meta.ID, Dest: &buf,
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bytes.Contains(buf.Bytes(), []byte("async-pull")) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pull payload = %q", buf.Bytes())
}

func TestRunPullUnknownVaultIsNoOp(t *testing.T) {
	t.Parallel()
	mgr, pullIn := distribution.New(distribution.Config{})
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

	pullIn <- distribution.PullRequest{
		VaultID: glid.New(), SegmentID: glid.New(), Dest: &bytes.Buffer{},
	}
	time.Sleep(50 * time.Millisecond)
}

func TestPromoteToHeadMissingSource(t *testing.T) {
	t.Parallel()
	_, err := distribution.PromoteToHead(filepath.Join(t.TempDir(), "missing"), t.TempDir())
	if err == nil {
		t.Fatal("expected rename error")
	}
}

func TestServePullUnknownVault(t *testing.T) {
	t.Parallel()
	mgr, _ := distribution.New(distribution.Config{})
	err := mgr.ServePull(distribution.PullRequest{
		VaultID: glid.New(), SegmentID: glid.New(), Dest: &bytes.Buffer{},
	})
	if !errors.Is(err, distribution.ErrUnknownVault) {
		t.Fatalf("ServePull() = %v, want ErrUnknownVault", err)
	}
}

func TestStreamSegmentGoneDuringPull(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	pub := &recordingPublisher{}
	mgr, _ := distribution.New(distribution.Config{})
	if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{Publisher: pub}); err != nil {
		t.Fatal(err)
	}
	seg := writeCompletedSegment(t, root, vaultID, "ephemeral")
	if err := mgr.PublishCompleted(context.Background(), seg); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(seg.Path); err != nil {
		t.Fatal(err)
	}
	err := mgr.ServePull(distribution.PullRequest{
		VaultID: vaultID, SegmentID: seg.Meta.ID, Dest: &bytes.Buffer{},
	})
	if !errors.Is(err, distribution.ErrSegmentGone) {
		t.Fatalf("ServePull() = %v, want ErrSegmentGone", err)
	}
}
