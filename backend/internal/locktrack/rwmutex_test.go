package locktrack

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// leakAnRLock acquires a read lock and returns without releasing — the
// orphaned-hold shape this package exists to diagnose. Named so the test can
// assert the leak report contains this function.
func leakAnRLock(m *RWMutex) {
	m.RLock()
}

func TestLeakedRLockIsReportedWithAcquisitionStack(t *testing.T) {
	t.Parallel()
	var m RWMutex
	m.EnableTracking()

	leakAnRLock(&m)

	leaks := m.Leaks(0)
	if len(leaks) != 1 {
		t.Fatalf("leaks = %d, want 1", len(leaks))
	}
	if leaks[0].Kind != HoldRead {
		t.Fatalf("kind = %s, want %s", leaks[0].Kind, HoldRead)
	}
	if !strings.Contains(leaks[0].Stack, "leakAnRLock") {
		t.Fatalf("leak stack does not name the acquisition site:\n%s", leaks[0].Stack)
	}
	// Reported once, not again.
	if again := m.Leaks(0); len(again) != 0 {
		t.Fatalf("re-reported %d leaks, want 0 (mark-once)", len(again))
	}
}

func TestStuckWriteWaiterIsReported(t *testing.T) {
	t.Parallel()
	var m RWMutex
	m.EnableTracking()

	m.RLock() // hold read so the writer queues
	acquired := make(chan struct{})
	go func() {
		m.Lock()
		close(acquired)
		m.Unlock()
	}()

	// Wait until the writer's wait entry is visible.
	deadline := time.Now().Add(2 * time.Second)
	for m.LiveCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("write waiter never registered")
		}
		time.Sleep(time.Millisecond)
	}
	leaks := m.Leaks(0)
	foundWait := false
	for _, l := range leaks {
		if l.Kind == WaitWrite {
			foundWait = true
		}
	}
	if !foundWait {
		t.Fatalf("no write-wait leak among %v", leaks)
	}

	m.RUnlock()
	<-acquired
	// After release + acquire + release, nothing should be tracked.
	deadline = time.Now().Add(2 * time.Second)
	for m.LiveCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("entries remain after clean release: %d", m.LiveCount())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBalancedUseLeavesNothingTracked(t *testing.T) {
	t.Parallel()
	var m RWMutex
	m.EnableTracking()

	m.RLock()
	m.RUnlock() //nolint:staticcheck // SA2001: empty critical sections exercise the tracker
	m.Lock()
	m.Unlock() //nolint:staticcheck // SA2001: see above
	if m.TryRLock() {
		m.RUnlock()
	}
	if m.TryLock() {
		m.Unlock()
	}
	if n := m.LiveCount(); n != 0 {
		t.Fatalf("tracked entries after balanced use = %d, want 0", n)
	}
	if leaks := m.Leaks(0); len(leaks) != 0 {
		t.Fatalf("leaks after balanced use: %v", leaks)
	}
}

func TestDisabledTrackingRecordsNothing(t *testing.T) {
	t.Parallel()
	var m RWMutex // tracking off

	m.RLock()
	// Deliberately never released while disabled: invisible by contract.
	if n := m.LiveCount(); n != 0 {
		t.Fatalf("disabled mutex tracked %d entries", n)
	}
	m.RUnlock()
}

func TestNestedReadHoldsMatchNewestFirst(t *testing.T) {
	t.Parallel()
	var m RWMutex
	m.EnableTracking()

	m.RLock()
	m.RLock()
	if n := m.LiveCount(); n != 2 {
		t.Fatalf("nested holds tracked = %d, want 2", n)
	}
	m.RUnlock()
	m.RUnlock()
	if n := m.LiveCount(); n != 0 {
		t.Fatalf("entries after nested release = %d, want 0", n)
	}
}

func TestConcurrentChurnNoFalseLeaks(t *testing.T) {
	t.Parallel()
	var m RWMutex
	m.EnableTracking()

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 500 {
				m.RLock()
				m.RUnlock() //nolint:staticcheck // SA2001: empty critical sections churn the tracker
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 100 {
				m.Lock()
				m.Unlock() //nolint:staticcheck // SA2001: see above
			}
		})
	}
	wg.Wait()
	if n := m.LiveCount(); n != 0 {
		t.Fatalf("entries after churn = %d, want 0", n)
	}
	if leaks := m.Leaks(0); len(leaks) != 0 {
		t.Fatalf("false leaks after churn: %d", len(leaks))
	}
}
