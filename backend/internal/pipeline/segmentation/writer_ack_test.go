package segmentation_test

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
)

func waitAck(t *testing.T, ack <-chan error, what string) {
	t.Helper()
	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("%s: ack returned error: %v", what, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: ack did not fire", what)
	}
}

// A lone ack-bearing record must be fsynced and acked promptly via group commit,
// without waiting for the (here: never-reached) fire-and-forget batch size or window.
func TestAckFiresAfterGroupCommitFsync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	startManager(t, segmentation.Config{
		SyncBatchSize:   1000,      // far beyond a single record
		SyncBatchWindow: time.Hour, // fire-and-forget window must not be the trigger
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		if in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
			t.Fatal(err)
		}
	})

	ack := make(chan error, 1)
	in <- segmentation.Input{Record: sampleRecord(0, time.Now().UTC()), Ack: ack}
	waitAck(t, ack, "lone ack record")

	if syncs.Load() == 0 {
		t.Fatal("expected a real fsync to back the ack (ack-after-fsync)")
	}
}

// With MaxCommitDelay set, a burst of ack records coalesces into a single fsync.
func TestAckCoalesceWithMaxCommitDelay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	startManager(t, segmentation.Config{
		SyncBatchSize:   1000,
		SyncBatchWindow: time.Hour,
		MaxCommitDelay:  200 * time.Millisecond,
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		if in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
			t.Fatal(err)
		}
	})

	const n = 5
	acks := make([]chan error, n)
	for i := range acks {
		acks[i] = make(chan error, 1)
		in <- segmentation.Input{Record: sampleRecord(uint32(i), time.Now().UTC()), Ack: acks[i]}
	}
	for i, ack := range acks {
		waitAck(t, ack, "coalesced record")
		_ = i
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("fsyncs = %d, want 1 (burst coalesced within MaxCommitDelay)", got)
	}
}

// A DisableFsync vault acks after the in-memory append and never fsyncs.
func TestDisableFsyncAcksWithoutSync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		if in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{DisableFsync: true}); err != nil {
			t.Fatal(err)
		}
	})

	ack := make(chan error, 1)
	in <- segmentation.Input{Record: sampleRecord(0, time.Now().UTC()), Ack: ack}
	waitAck(t, ack, "disable-fsync record")

	time.Sleep(50 * time.Millisecond)
	if got := syncs.Load(); got != 0 {
		t.Fatalf("fsyncs = %d, want 0 on a DisableFsync vault", got)
	}
	entries, err := os.ReadDir(paths.WorkingDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("working segments = %d, want 1 (record still written to the file)", len(entries))
	}
}

// DisableFsync is a per-vault override: one vault on a manager can disable fsync
// while another inherits the (fsync-on) default.
func TestPerVaultDisableFsyncOverride(t *testing.T) {
	t.Parallel()
	dirOn := t.TempDir()
	dirOff := t.TempDir()
	vaultOn := glid.New()
	vaultOff := glid.New()

	var syncs atomic.Uint32
	var inOn, inOff chan<- segmentation.Input
	startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		if inOn, err = mgr.RegisterVault(vaultOn, dirOn, segmentation.VaultConfig{}); err != nil {
			t.Fatal(err)
		}
		if inOff, err = mgr.RegisterVault(vaultOff, dirOff, segmentation.VaultConfig{DisableFsync: true}); err != nil {
			t.Fatal(err)
		}
	})

	ackOff := make(chan error, 1)
	inOff <- segmentation.Input{Record: sampleRecord(0, time.Now().UTC()), Ack: ackOff}
	waitAck(t, ackOff, "fsync-disabled vault")
	time.Sleep(50 * time.Millisecond)
	if got := syncs.Load(); got != 0 {
		t.Fatalf("fsyncs = %d after disabled-vault write, want 0", got)
	}

	ackOn := make(chan error, 1)
	inOn <- segmentation.Input{Record: sampleRecord(0, time.Now().UTC()), Ack: ackOn}
	waitAck(t, ackOn, "fsync-enabled vault")
	if got := syncs.Load(); got < 1 {
		t.Fatalf("fsyncs = %d after enabled-vault write, want >= 1", got)
	}
}

// An un-encodable record nacks its ack instead of dropping it silently.
func TestEncodeErrorNacksAck(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()

	var in chan<- segmentation.Input
	startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		if in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
			t.Fatal(err)
		}
	})

	ack := make(chan error, 1)
	in <- segmentation.Input{Record: nil, Ack: ack} // nil record fails EncodeFrame
	select {
	case err := <-ack:
		if err == nil {
			t.Fatal("expected a nack for an un-encodable record")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("encode error did not nack the ack")
	}
}
