package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
)

// The vault-ctl Raft apply pump reads pipeline registration state while
// applying an entry (onSeal -> ackOwnHolderReceipt -> pipelineVaultChunkRoot,
// onRequestDelete -> deleteLocalCopy -> pipelineVaultChunkRoot). hashicorp/raft
// runs exactly ONE FSM goroutine per group, so a handler that blocks on o.mu
// stops every apply for that group. A caller that holds o.mu while waiting for
// one of those applies then waits forever: the entry it is waiting for can only
// be applied by the goroutine its own lock is blocking. That cycle wedged a
// node permanently, taking every other o.mu consumer down with it.
//
// These tests hold o.mu in the mode that closes the cycle and require the
// pump-path readers to answer anyway. The deadline is a deadlock detector, not
// a performance assertion: the correct implementation never touches the lock,
// so it returns immediately, while a regression blocks forever.
const applyPumpLockDeadline = 10 * time.Second

func mustAnswerWhileLocked(t *testing.T, name string, read func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		read()
	}()
	select {
	case <-done:
	case <-time.After(applyPumpLockDeadline):
		t.Fatalf("%s blocked while o.mu was held: the Raft apply pump calls this, "+
			"so taking o.mu here deadlocks the node", name)
	}
}

// TestPipelineReadersDoNotBlockOnOrchestratorWriteLock pins the pump-path
// readers against a HELD WRITE LOCK — the shape of a config apply or placement
// reload running concurrently with an apply.
func TestPipelineReadersDoNotBlockOnOrchestratorWriteLock(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-1", SegmentsDir: t.TempDir()})
	vaultID := glid.New()
	o.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true, hasHandle: true})

	o.mu.Lock()
	defer o.mu.Unlock()

	mustAnswerWhileLocked(t, "pipelineVaultChunkRoot", func() {
		if _, ok := o.pipelineVaultChunkRoot(vaultID); !ok {
			t.Errorf("pipelineVaultChunkRoot: registered home vault reported absent")
		}
	})
	mustAnswerWhileLocked(t, "pipelineVaultStagingRoot", func() {
		if _, ok := o.pipelineVaultStagingRoot(vaultID); !ok {
			t.Errorf("pipelineVaultStagingRoot: registered vault reported absent")
		}
	})
	mustAnswerWhileLocked(t, "isPipelineIngestVault", func() {
		if !o.isPipelineIngestVault(vaultID) {
			t.Errorf("isPipelineIngestVault: registered vault reported absent")
		}
	})
}

// TestPipelineReadersDoNotBlockBehindQueuedWriter is the case that makes RLock
// an insufficient fix. Go's RWMutex blocks a new RLock behind a WAITING writer,
// so a reader-holding appender plus one queued writer is enough to wedge a
// pump-path RLock — the same mechanism as the schedulePostSeal recursive-RLock
// node freeze.
func TestPipelineReadersDoNotBlockBehindQueuedWriter(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-1", SegmentsDir: t.TempDir()})
	vaultID := glid.New()
	o.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true, hasHandle: true})

	// Reader held (the appender awaiting its Raft apply).
	o.mu.RLock()
	defer o.mu.RUnlock()

	// Writer queued behind it and parked for the duration; it can never be
	// granted, since the reader above is held until the test returns.
	writerQueued := make(chan struct{})
	go func() {
		close(writerQueued)
		o.mu.Lock()
		o.mu.Unlock()
	}()
	<-writerQueued

	mustAnswerWhileLocked(t, "pipelineVaultChunkRoot", func() {
		if _, ok := o.pipelineVaultChunkRoot(vaultID); !ok {
			t.Errorf("pipelineVaultChunkRoot: registered home vault reported absent")
		}
	})
	mustAnswerWhileLocked(t, "isPipelineIngestVault", func() {
		if !o.isPipelineIngestVault(vaultID) {
			t.Errorf("isPipelineIngestVault: registered vault reported absent")
		}
	})
}

// TestPipelineRegistrationCopyOnWrite pins the property the lock-free readers
// rely on: writes are copy-on-write, so a map a reader already holds is never
// mutated underneath it, and the zero value (nothing published yet) reads as
// empty rather than panicking.
func TestPipelineRegistrationCopyOnWrite(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-1", SegmentsDir: t.TempDir()})

	if _, ok := o.lookupPipelineVault(glid.New()); ok {
		t.Fatalf("unpublished registrations must read as absent")
	}

	a, b := glid.New(), glid.New()
	o.setPipelineVaultLocked(a, pipelineVaultReg{home: true, hasHandle: true})

	// A reader that grabbed the map before the next write must keep observing
	// exactly what it read — this is what makes an unlocked read safe.
	held := o.pipelineVaultsMap()

	o.setPipelineVaultLocked(b, pipelineVaultReg{home: false})
	o.setPipelineVaultLocked(a, pipelineVaultReg{home: false, hasHandle: true})
	o.deletePipelineVaultLocked(a)

	if len(held) != 1 {
		t.Errorf("previously published map mutated: len = %d, want 1", len(held))
	}
	if got, ok := held[a]; !ok || !got.home {
		t.Errorf("previously published map mutated: entry for a = %+v (ok=%v), want home:true", got, ok)
	}

	if _, ok := o.lookupPipelineVault(a); ok {
		t.Errorf("lookupPipelineVault still reports the deleted vault")
	}
	if reg, ok := o.lookupPipelineVault(b); !ok || reg.home {
		t.Errorf("lookupPipelineVault(b) = %+v (ok=%v), want the registered non-home entry", reg, ok)
	}

	o.deletePipelineVaultLocked(b)
	if n := len(o.pipelineVaultsMap()); n != 0 {
		t.Errorf("after deleting every vault, %d registrations remain", n)
	}
}
