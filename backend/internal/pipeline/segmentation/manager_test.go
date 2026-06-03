package segmentation_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
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
	var in chan<- *record.Record
	_, _ = startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:            func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir)
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	in <- sampleRecord(0, ts)
	waitSync(t, &syncs, 1)

	entries, err := os.ReadDir(filepath.Join(dir, "working"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments = %d, want 1", len(entries))
	}

	path := filepath.Join(dir, "working", entries[0].Name())
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
	var in chan<- *record.Record
	_, _ = startManager(t, segmentation.Config{
		SyncBatchSize:   4,
		SyncBatchWindow: time.Hour,
		OnSync:            func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir)
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	for i := range 4 {
		in <- sampleRecord(uint32(i), ts.Add(time.Duration(i)*time.Millisecond))
	}
	waitSync(t, &syncs, 1)
	if syncs.Load() != 1 {
		t.Fatalf("sync count = %d, want 1 batch fsync for 4 records", syncs.Load())
	}
}

func TestManagerClosesOnSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var in chan<- *record.Record
	_, completed := startManager(t, segmentation.Config{
		ClosePolicy:       segmentation.ClosePolicy{MaxBytes: 256},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:      4,
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir)
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	for i := range 8 {
		in <- sampleRecord(uint32(i), ts.Add(time.Duration(i)*time.Millisecond))
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
		if _, err := os.Stat(filepath.Join(dir, "working", seg.Meta.ID.String())); !os.IsNotExist(err) {
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
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for completed segment")
	}
}

func TestManagerClosesOnAge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(now.UnixNano())

	var in chan<- *record.Record
	_, completed := startManager(t, segmentation.Config{
		ClosePolicy:       segmentation.ClosePolicy{MaxAge: time.Minute},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:      4,
		Now: func() time.Time {
			return time.Unix(0, clock.Load()).UTC()
		},
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir)
		if err != nil {
			t.Fatal(err)
		}
	})

	in <- sampleRecord(0, now)
	time.Sleep(20 * time.Millisecond)

	clock.Add(int64(time.Minute))

	in <- sampleRecord(1, now.Add(time.Second))
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for age-based close")
	}
}

func TestManagerPerVaultIsolation(t *testing.T) {
	t.Parallel()
	dirA := t.TempDir()
	dirB := t.TempDir()
	vaultA := glid.New()
	vaultB := glid.New()

	var syncs atomic.Uint32
	var inA, inB chan<- *record.Record
	_, _ = startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:            func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		inA, err = mgr.RegisterVault(vaultA, dirA)
		if err != nil {
			t.Fatal(err)
		}
		inB, err = mgr.RegisterVault(vaultB, dirB)
		if err != nil {
			t.Fatal(err)
		}
	})

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	inA <- sampleRecord(0, ts)
	inB <- sampleRecord(0, ts.Add(time.Second))
	waitSync(t, &syncs, 2)

	for _, dir := range []string{dirA, dirB} {
		entries, err := os.ReadDir(filepath.Join(dir, "working"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("%s: working segments = %d", dir, len(entries))
		}
	}
}

func TestManagerDoesNotCloseEmptySegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	_, completed := startManager(t, segmentation.Config{
		ClosePolicy:       segmentation.ClosePolicy{MaxBytes: 64},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		CompletedCap:      1,
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		if _, err := mgr.RegisterVault(vaultID, dir); err != nil {
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
	if _, err := mgr.RegisterVault(glid.New(), dir); err != nil {
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

	if err := mgr.Run(ctx); err != segmentation.ErrNotRunning {
		t.Fatalf("Run() = %v, want ErrNotRunning", err)
	}
}

func TestManagerRegisterDuringRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- *record.Record
	mgr, _ := segmentation.New(segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:            func() { syncs.Add(1) },
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
	in, err = mgr.RegisterVault(vaultID, dir)
	if err != nil {
		t.Fatalf("RegisterVault during Run: %v", err)
	}

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	in <- sampleRecord(0, ts)
	waitSync(t, &syncs, 1)
}

func TestManagerRegisterAfterRunFinished(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, _ := segmentation.New(segmentation.Config{})
	if _, err := mgr.RegisterVault(glid.New(), dir); err != nil {
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

	_, err := mgr.RegisterVault(glid.New(), t.TempDir())
	if err != segmentation.ErrNotRunning {
		t.Fatalf("RegisterVault() = %v, want ErrNotRunning", err)
	}
}
