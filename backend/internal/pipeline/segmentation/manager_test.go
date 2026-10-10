package segmentation_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
	"gastrolog/internal/waittest"
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

// appendProgress snapshots every vault writer's stage counters and the
// manager's dropped-record count.
func appendProgress(mgr *segmentation.Manager) func() string {
	return func() string {
		var b strings.Builder
		for _, s := range mgr.AppendStats() {
			fmt.Fprintf(&b, "vault %s appended=%d durable=%d queue=%d completed=%d; ",
				s.VaultID, s.RecordsAppended, s.RecordsDurable, s.QueueDepth, s.SegmentsCompleted)
		}
		fmt.Fprintf(&b, "dropped=%d", mgr.DroppedRecords())
		return b.String()
	}
}

func waitSync(t *testing.T, mgr *segmentation.Manager, syncs *atomic.Uint32, want uint32) {
	t.Helper()
	progress := appendProgress(mgr)
	waittest.Progress(t, fmt.Sprintf("%d fsyncs", want), func() (string, bool) {
		n := syncs.Load()
		return fmt.Sprintf("syncs=%d %s", n, progress()), n >= want
	})
}

func waitDurable(t *testing.T, mgr *segmentation.Manager, vaultID glid.GLID, want uint64) {
	t.Helper()
	progress := appendProgress(mgr)
	waittest.Progress(t, fmt.Sprintf("%d records durable", want), func() (string, bool) {
		for _, s := range mgr.AppendStats() {
			if s.VaultID == vaultID && s.RecordsDurable >= want {
				return "", true
			}
		}
		return progress(), false
	})
}

func TestManagerAppendsToWorkingSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	mgr, _ := startManager(t, segmentation.Config{
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
	waitSync(t, mgr, &syncs, 1)

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
	mgr, _ := startManager(t, segmentation.Config{
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
	waitSync(t, mgr, &syncs, 1)
	if syncs.Load() != 1 {
		t.Fatalf("sync count = %d, want 1 batch fsync for 4 records", syncs.Load())
	}
}

func TestManagerCompletesOnSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var in chan<- segmentation.Input
	mgr, completed := startManager(t, segmentation.Config{
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

	seg := waittest.Recv(t, "size-completed segment", completed, appendProgress(mgr))
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
}

// TestManagerCountsSegmentsCompleted: AppendStats reports the
// segments-completed stage counter, incremented once per working/ →
// completed/ promotion.
func TestManagerCountsSegmentsCompleted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var in chan<- segmentation.Input
	mgr, completed := startManager(t, segmentation.Config{
		CompletePolicy:  segmentation.CompletePolicy{MaxBytes: 128},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:    8,
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	// Enough records to force at least two rotations (MaxBytes=128).
	for i := range 64 {
		in <- segmentation.Input{Record: sampleRecord(uint32(i), ts.Add(time.Duration(i)*time.Millisecond))}
	}
	// Drain two completions to know rotation happened.
	for range 2 {
		waittest.Recv(t, "completed segment", completed, appendProgress(mgr))
	}

	// The counter increments before the completion is announced.
	stats := mgr.AppendStats()
	if len(stats) != 1 || stats[0].SegmentsCompleted < 2 {
		t.Fatalf("SegmentsCompleted after two announced completions: %+v, want one vault at >= 2", stats)
	}
	if stats[0].VaultID != vaultID {
		t.Fatalf("stats vault = %s, want %s", stats[0].VaultID, vaultID)
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
	mgr, completed := startManager(t, segmentation.Config{
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
	waitDurable(t, mgr, vaultID, 1)

	clock.Add(int64(time.Minute))

	in <- segmentation.Input{Record: sampleRecord(1, now.Add(time.Second))}
	waittest.Recv(t, "age-based completion", completed, appendProgress(mgr))
}

func TestManagerPerVaultIsolation(t *testing.T) {
	t.Parallel()
	dirA := t.TempDir()
	dirB := t.TempDir()
	vaultA := glid.New()
	vaultB := glid.New()

	var syncs atomic.Uint32
	var inA, inB chan<- segmentation.Input
	mgr, _ := startManager(t, segmentation.Config{
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
	waitSync(t, mgr, &syncs, 2)

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

	mgr, completed := segmentation.New(segmentation.Config{
		CompletePolicy:  segmentation.CompletePolicy{MaxBytes: 64},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:    1,
	})
	if _, err := mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	cancel()
	<-done

	// Run closes the channel on exit after the final flush, so this sees every
	// completion of the manager's lifetime, shutdown included.
	if seg, ok := <-completed; ok {
		t.Fatalf("unexpected completed segment: %+v", seg)
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
	mgr, _ := segmentation.New(segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	})
	startRunning(t, mgr)
	syncsBefore := syncs.Load()

	in, err := mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
	if err != nil {
		t.Fatalf("RegisterVault during Run: %v", err)
	}

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	in <- segmentation.Input{Record: sampleRecord(0, ts)}
	waitSync(t, mgr, &syncs, syncsBefore+1)
}

// startRunning starts mgr.Run and returns once Run is active: a sentinel
// vault registered before Run gets its writer from Run itself, so the
// sentinel's first ack orders after Run has taken over registration.
func startRunning(t *testing.T, mgr *segmentation.Manager) {
	t.Helper()
	sentinel, err := mgr.RegisterVault(glid.New(), t.TempDir(), segmentation.VaultConfig{})
	if err != nil {
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
	ack := make(chan error, 1)
	sentinel <- segmentation.Input{Record: sampleRecord(0, time.Now().UTC()), Ack: ack}
	if err := waittest.Recv(t, "sentinel ack from a Run-started writer", ack, appendProgress(mgr)); err != nil {
		t.Fatalf("sentinel ack: %v", err)
	}
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
	startRunning(t, mgr)

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

// --- working/ restart recovery ---

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
