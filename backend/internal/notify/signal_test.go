package notify

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSignalNotifyWakesWaiter(t *testing.T) {
	t.Parallel()
	s := NewSignal()
	ch := s.C()

	done := make(chan struct{})
	go func() {
		<-ch
		close(done)
	}()

	s.Notify()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waiter not woken within 1s")
	}
}

func TestSignalNotifyWakesMultipleWaiters(t *testing.T) {
	t.Parallel()
	s := NewSignal()
	const n = 10

	var woken atomic.Int32
	var wg sync.WaitGroup
	for range n {
		ch := s.C()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ch
			woken.Add(1)
		}()
	}

	s.Notify()
	wg.Wait()

	if got := woken.Load(); got != n {
		t.Fatalf("expected %d woken, got %d", n, got)
	}
}

func TestSignalRequiresReCallAfterReceive(t *testing.T) {
	t.Parallel()
	s := NewSignal()
	ch1 := s.C()
	s.Notify()
	<-ch1 // first notification received

	// ch1 is stale — it stays closed, so receiving again returns immediately.
	select {
	case <-ch1:
		// expected: stale channel is already closed
	default:
		t.Fatal("stale channel should be readable (closed)")
	}

	// A fresh C() call returns a new, open channel.
	ch2 := s.C()
	select {
	case <-ch2:
		t.Fatal("fresh channel should block until next Notify")
	default:
		// expected: new channel blocks
	}

	// Second notification wakes ch2.
	s.Notify()
	select {
	case <-ch2:
	default:
		t.Fatal("ch2 should be closed after second Notify")
	}
}

func TestSignalConcurrentNotify(t *testing.T) {
	t.Parallel()
	s := NewSignal()
	const notifiers = 10
	const notifiesPerGoroutine = 1000

	// Concurrent reader that re-calls C() on each wakeup.
	stop := make(chan struct{})
	var received atomic.Int64
	var readerDone sync.WaitGroup
	readerDone.Add(1)
	go func() {
		defer readerDone.Done()
		for {
			select {
			case <-s.C():
				received.Add(1)
			case <-stop:
				return
			}
		}
	}()

	// Concurrent notifiers.
	var wg sync.WaitGroup
	for range notifiers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range notifiesPerGoroutine {
				s.Notify()
			}
		}()
	}

	wg.Wait()
	close(stop)
	readerDone.Wait()

	// The reader may not catch every notification (it can miss rapid
	// back-to-back notifies while re-calling C()), but it must have
	// received at least one.
	if received.Load() == 0 {
		t.Fatal("reader received zero notifications")
	}
}

func TestSignalNotifyWithoutWaiters(t *testing.T) {
	t.Parallel()
	s := NewSignal()
	// Must not panic when notifying with no waiters.
	s.Notify()
	s.Notify()
	s.Notify()

	// A subsequent waiter should still work.
	ch := s.C()
	s.Notify()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("waiter not woken after no-waiter notifies")
	}
}
