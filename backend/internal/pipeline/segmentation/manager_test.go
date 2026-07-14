package segmentation_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

func sampleRecord(seq uint32, ts time.Time) *record.Record {
	ingester := glid.New()
	node := glid.New()
	return &record.Record{
		SourceTS: ts.Add(-time.Second),
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: ingester,
			NodeID:     node,
			IngestTS:   ts,
			IngestSeq:  seq,
		},
		Attrs: record.Attributes{"env": "prod"},
		Raw:   []byte("log line"),
	}
}

func startManager(t *testing.T, cfg segmentation.Config, register func(t *testing.T, mgr *segmentation.Manager)) (*segmentation.Manager, <-chan segmentation.CompletedSegment) {
	t.Helper()
	mgr, completed := segmentation.New(cfg)
	register(t, mgr)
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
	return mgr, completed
}

func waitSync(t *testing.T, syncs *atomic.Uint32, want uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if syncs.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("sync count = %d, want >= %d", syncs.Load(), want)
}

func TestManagerAppendsToWorkingSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	_, _ = startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	in <- segmentation.Input{Record: sampleRecord(0, ts)}
	waitSync(t, &syncs, 1)

	entries, err := os.ReadDir(paths.WorkingDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments = %d, want 1", len(entries))
	}

	path := filepath.Join(paths.WorkingDir(dir), entries[0].Name())
	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()

	got, err := sf.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records", len(got))
	}
	if got[0].Attrs["env"] != "prod" {
		t.Errorf("attrs = %v", got[0].Attrs)
	}
}

func TestManagerGroupSyncBatchesFsync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	_, _ = startManager(t, segmentation.Config{
		SyncBatchSize:   4,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	for i := range 4 {
		in <- segmentation.Input{Record: sampleRecord(uint32(i), ts.Add(time.Duration(i)*time.Millisecond))}
	}
	waitSync(t, &syncs, 1)
	if syncs.Load() != 1 {
		t.Fatalf("sync count = %d, want 1 batch fsync for 4 records", syncs.Load())
	}
}

func TestManagerCompletesOnSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var in chan<- segmentation.Input
	_, completed := startManager(t, segmentation.Config{
		CompletePolicy:  segmentation.CompletePolicy{MaxBytes: 256},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:    4,
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	for i := range 8 {
		in <- segmentation.Input{Record: sampleRecord(uint32(i), ts.Add(time.Duration(i)*time.Millisecond))}
	}

	select {
	case seg := <-completed:
		if seg.VaultID != vaultID {
			t.Fatalf("vault = %s", seg.VaultID)
		}
		if seg.Header.Flags&segment.FlagComplete == 0 {
			t.Error("expected FlagComplete on closed segment")
		}
		if _, err := os.Stat(seg.Path); err != nil {
			t.Fatalf("completed path: %v", err)
		}
		if _, err := os.Stat(paths.WorkingSegment(dir, seg.SegmentID)); !os.IsNotExist(err) {
			t.Fatalf("working copy should be gone: %v", err)
		}
		sf, err := segment.Open(seg.Path)
		if err != nil {
			t.Fatal(err)
		}
		defer sf.Close()
		if sf.Header().RecordCount == 0 {
			t.Fatal("completed segment has no records")
		}
		if sf.Header().IndexOffset == 0 {
			t.Fatal("completed segment missing EventID index")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for completed segment")
	}
}

func TestManagerCompletesOnAge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(now.UnixNano())

	var in chan<- segmentation.Input
	_, completed := startManager(t, segmentation.Config{
		CompletePolicy:  segmentation.CompletePolicy{MaxAge: time.Minute},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:    4,
		Now: func() time.Time {
			return time.Unix(0, clock.Load()).UTC()
		},
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
	})

	in <- segmentation.Input{Record: sampleRecord(0, now)}
	time.Sleep(20 * time.Millisecond)

	clock.Add(int64(time.Minute))

	in <- segmentation.Input{Record: sampleRecord(1, now.Add(time.Second))}
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for age-based completion")
	}
}

func TestManagerPerVaultIsolation(t *testing.T) {
	t.Parallel()
	dirA := t.TempDir()
	dirB := t.TempDir()
	vaultA := glid.New()
	vaultB := glid.New()

	var syncs atomic.Uint32
	var inA, inB chan<- segmentation.Input
	_, _ = startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		inA, err = mgr.RegisterVault(vaultA, dirA, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
		inB, err = mgr.RegisterVault(vaultB, dirB, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	inA <- segmentation.Input{Record: sampleRecord(0, ts)}
	inB <- segmentation.Input{Record: sampleRecord(0, ts.Add(time.Second))}
	waitSync(t, &syncs, 2)

	for _, dir := range []string{dirA, dirB} {
		entries, err := os.ReadDir(paths.WorkingDir(dir))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("%s: working segments = %d", dir, len(entries))
		}
	}
}

func TestManagerDoesNotCompleteEmptySegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	_, completed := startManager(t, segmentation.Config{
		CompletePolicy:  segmentation.CompletePolicy{MaxBytes: 64},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:    1,
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		if _, err := mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
			t.Fatal(err)
		}
	})

	select {
	case seg := <-completed:
		t.Fatalf("unexpected completed segment: %+v", seg)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestManagerRunTwice(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, _ := segmentation.New(segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
	})
	if _, err := mgr.RegisterVault(glid.New(), dir, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if err := mgr.Run(ctx); err != segmentation.ErrAlreadyRunning {
		t.Fatalf("Run() = %v, want ErrAlreadyRunning", err)
	}
}

func TestManagerRegisterDuringRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	mgr, _ := segmentation.New(segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	})

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

	time.Sleep(20 * time.Millisecond)

	var err error
	in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
	if err != nil {
		t.Fatalf("RegisterVault during Run: %v", err)
	}

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	in <- segmentation.Input{Record: sampleRecord(0, ts)}
	waitSync(t, &syncs, 1)
}

func TestManagerRegisterAfterRunFinished(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, _ := segmentation.New(segmentation.Config{})
	if _, err := mgr.RegisterVault(glid.New(), dir, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	_, err := mgr.RegisterVault(glid.New(), t.TempDir(), segmentation.VaultConfig{})
	if err != segmentation.ErrNotRunning {
		t.Fatalf("RegisterVault() = %v, want ErrNotRunning", err)
	}
}

func TestManagerUnregisterVault(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()
	mgr, _ := segmentation.New(segmentation.Config{})
	if _, err := mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}
	mgr.UnregisterVault(vaultID)
	entries, err := os.ReadDir(paths.WorkingDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("working segments after unregister = %d, want 0", len(entries))
	}
	if _, err := mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
		t.Fatalf("re-register after unregister: %v", err)
	}
	entries, err = os.ReadDir(paths.WorkingDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments after re-register = %d, want 1", len(entries))
	}
}

func TestManagerUnregisterVaultDuringRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()
	mgr, _ := segmentation.New(segmentation.Config{})

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

	time.Sleep(20 * time.Millisecond)

	if _, err := mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}
	mgr.UnregisterVault(vaultID)

	entries, err := os.ReadDir(paths.WorkingDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("working segments after unregister during run = %d, want 0", len(entries))
	}
}

// --- working/ restart recovery (gastrolog-1sylj7) ---

// seedWorkingSegment simulates a crashed writer: records appended and fsynced
// (and therefore ACKED) into working/<segID>, process killed before the complete
// policy fired. The file is deliberately left unclosed and unfinalized — the
// exact on-disk state a crash leaves behind.
func seedWorkingSegment(t *testing.T, root string, vaultID glid.GLID, n int) glid.GLID {
	t.Helper()
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		t.Fatal(err)
	}
	segID := glid.New()
	sf, err := segment.Create(paths.WorkingSegment(root, segID), segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 7, 4, 3, 0, 0, 0, time.UTC)
	for i := range n {
		if err := sf.Append(sampleRecord(uint32(i), ts.Add(time.Duration(i)*time.Second)), ts); err != nil { //nolint:gosec // test loop index
			t.Fatal(err)
		}
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	// No Finalize, no Close: crash.
	return segID
}

// Acked records in an orphaned working segment must become a completed
// segment on re-register — losing them is a cardinal-rule violation.
func TestRegisterVaultRecoversOrphanedWorkingSegment(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	vaultID := glid.New()
	segID := seedWorkingSegment(t, root, vaultID, 3)

	mgr, completed := segmentation.New(segmentation.Config{})
	if _, err := mgr.RegisterVault(vaultID, root, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	if _, err := os.Stat(paths.WorkingSegment(root, segID)); !os.IsNotExist(err) {
		t.Fatalf("working/ orphan still present after recovery (err=%v)", err)
	}
	completedPath := paths.CompletedSegment(root, segID)
	sf, err := segment.Open(completedPath)
	if err != nil {
		t.Fatalf("open recovered completed segment: %v", err)
	}
	defer sf.Close()
	if got := sf.Header().RecordCount; got != 3 {
		t.Fatalf("recovered RecordCount = %d, want 3", got)
	}

	select {
	case cs := <-completed:
		if cs.SegmentID != segID || cs.VaultID != vaultID {
			t.Fatalf("completed notification = %+v, want segment %s vault %s", cs, segID, vaultID)
		}
		if cs.Header.RecordCount != 3 {
			t.Fatalf("notification RecordCount = %d, want 3", cs.Header.RecordCount)
		}
	default:
		t.Fatal("recovered segment was not announced on the completed channel")
	}
}

// An empty orphan (crash before any append) holds no acked data; recovery
// discards it instead of publishing an empty segment.
func TestRegisterVaultDiscardsEmptyWorkingOrphan(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	vaultID := glid.New()
	segID := seedWorkingSegment(t, root, vaultID, 0)

	mgr, completed := segmentation.New(segmentation.Config{})
	if _, err := mgr.RegisterVault(vaultID, root, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	if _, err := os.Stat(paths.WorkingSegment(root, segID)); !os.IsNotExist(err) {
		t.Fatalf("empty orphan still present (err=%v)", err)
	}
	if _, err := os.Stat(paths.CompletedSegment(root, segID)); !os.IsNotExist(err) {
		t.Fatalf("empty orphan promoted to completed/ (err=%v)", err)
	}
	select {
	case cs := <-completed:
		t.Fatalf("empty orphan announced: %+v", cs)
	default:
	}
}

// A torn tail (crash mid-append after the last fsync) recovers the synced
// prefix: the acked records survive, the partial frame is dropped.
func TestRegisterVaultRecoversTornTailWorkingOrphan(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	vaultID := glid.New()
	segID := seedWorkingSegment(t, root, vaultID, 2)

	// Simulate the torn frame: raw garbage appended after the synced prefix.
	f, err := os.OpenFile(paths.WorkingSegment(root, segID), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	mgr, completed := segmentation.New(segmentation.Config{})
	if _, err := mgr.RegisterVault(vaultID, root, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	sf, err := segment.Open(paths.CompletedSegment(root, segID))
	if err != nil {
		t.Fatalf("open recovered completed segment: %v", err)
	}
	defer sf.Close()
	if got := sf.Header().RecordCount; got != 2 {
		t.Fatalf("recovered RecordCount = %d, want 2 (synced prefix)", got)
	}
	select {
	case cs := <-completed:
		if cs.Header.RecordCount != 2 {
			t.Fatalf("notification RecordCount = %d, want 2", cs.Header.RecordCount)
		}
	default:
		t.Fatal("torn-tail orphan was not announced after recovery")
	}
}
