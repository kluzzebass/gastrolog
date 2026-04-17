package orchestrator

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func drainSubscription(t *testing.T, sub *JobSubscription, want int, timeout time.Duration) []JobEvent {
	t.Helper()
	var got []JobEvent
	deadline := time.Now().Add(timeout)
	for len(got) < want && time.Now().Before(deadline) {
		select {
		case evt, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, evt)
		case <-time.After(20 * time.Millisecond):
		}
	}
	return got
}

func ev(kind JobEventKind, name string) JobEvent {
	return JobEvent{Kind: kind, Job: JobInfo{Name: name}}
}

// TestJobEventBroker_SingleSubscriber verifies basic delivery from one
// Publish to one Subscribe.
func TestJobEventBroker_SingleSubscriber(t *testing.T) {
	b := NewJobEventBroker(16)
	sub, cancel := b.Subscribe()
	defer cancel()

	b.Publish(ev(JobEventScheduled, "job-a"))
	got := drainSubscription(t, sub, 1, time.Second)
	if len(got) != 1 || got[0].Kind != JobEventScheduled || got[0].Job.Name != "job-a" {
		t.Errorf("unexpected events: %+v", got)
	}
}

// TestJobEventBroker_FanOut verifies two subscribers both receive every
// event.
func TestJobEventBroker_FanOut(t *testing.T) {
	b := NewJobEventBroker(16)
	sa, cancelA := b.Subscribe()
	defer cancelA()
	sb, cancelB := b.Subscribe()
	defer cancelB()

	for i := 0; i < 3; i++ {
		b.Publish(ev(JobEventScheduled, "job"))
	}
	gotA := drainSubscription(t, sa, 3, time.Second)
	gotB := drainSubscription(t, sb, 3, time.Second)
	if len(gotA) != 3 || len(gotB) != 3 {
		t.Errorf("fan-out counts: A=%d B=%d, want 3 each", len(gotA), len(gotB))
	}
}

// TestJobEventBroker_SlowSubscriberDropsRatherThanBlocks verifies that a
// subscriber that doesn't drain doesn't block publishes or other
// subscribers.
func TestJobEventBroker_SlowSubscriberDropsRatherThanBlocks(t *testing.T) {
	b := NewJobEventBroker(4) // small buffer to force drops

	slow, cancelSlow := b.Subscribe()
	defer cancelSlow()
	fast, cancelFast := b.Subscribe()
	defer cancelFast()

	// Publish way more than the buffer can hold. slow never reads.
	start := time.Now()
	const total = 1000
	for i := 0; i < total; i++ {
		b.Publish(ev(JobEventScheduled, "job"))
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Errorf("Publish stalled (slow subscriber blocked others): %v for %d events", elapsed, total)
	}

	// Fast subscriber still receives — drain what's in its buffer quickly.
	gotFast := drainSubscription(t, fast, 4, time.Second)
	if len(gotFast) == 0 {
		t.Error("fast subscriber received nothing")
	}

	// Slow subscriber's drop counter bumped.
	if dropped := slow.Dropped(); dropped == 0 {
		t.Errorf("slow subscriber dropped=0, expected > 0 (total=%d)", total)
	}
}

// TestJobEventBroker_CancelClosesChannel verifies cancel() closes the
// receive channel cleanly.
func TestJobEventBroker_CancelClosesChannel(t *testing.T) {
	b := NewJobEventBroker(4)
	sub, cancel := b.Subscribe()

	cancel()

	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("expected channel closed, got a value")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after cancel")
	}
}

// TestJobEventBroker_CancelTwiceSafe verifies double-cancel is a no-op.
func TestJobEventBroker_CancelTwiceSafe(t *testing.T) {
	b := NewJobEventBroker(4)
	_, cancel := b.Subscribe()
	cancel()
	cancel() // must not panic
}

// TestJobEventBroker_PublishAfterCancel verifies publishing to a broker
// whose only subscriber was cancelled is a no-op (no panic on closed
// channel).
func TestJobEventBroker_PublishAfterCancel(t *testing.T) {
	b := NewJobEventBroker(4)
	_, cancel := b.Subscribe()
	cancel()
	b.Publish(ev(JobEventScheduled, "after-cancel")) // must not panic
}

// TestJobEventBroker_Close closes the broker and verifies subscribers see
// channel closure.
func TestJobEventBroker_Close(t *testing.T) {
	b := NewJobEventBroker(4)
	sub, _ := b.Subscribe()

	b.Close()

	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("expected closed channel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after broker.Close")
	}

	// Publish after Close is a no-op.
	b.Publish(ev(JobEventScheduled, "post-close"))

	// Subscribe after Close returns an already-closed channel.
	late, cancelLate := b.Subscribe()
	defer cancelLate()
	select {
	case _, ok := <-late.Events():
		if ok {
			t.Error("expected late subscription's channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("late subscription channel not closed")
	}
}

// TestJobEventBroker_ConcurrentPublishSubscribe hammers the broker from
// multiple goroutines to surface data races.
func TestJobEventBroker_ConcurrentPublishSubscribe(t *testing.T) {
	b := NewJobEventBroker(64)

	const publishers = 4
	const subscribers = 4
	const eventsEach = 250

	var wg sync.WaitGroup
	// Subscribers.
	received := make([]*atomic.Int64, subscribers)
	for i := 0; i < subscribers; i++ {
		received[i] = new(atomic.Int64)
		sub, cancel := b.Subscribe()
		wg.Add(1)
		go func(sub *JobSubscription, counter *atomic.Int64, cancel func()) {
			defer wg.Done()
			defer cancel()
			timeout := time.After(3 * time.Second)
			for {
				select {
				case _, ok := <-sub.Events():
					if !ok {
						return
					}
					counter.Add(1)
				case <-timeout:
					return
				}
			}
		}(sub, received[i], cancel)
	}

	// Publishers.
	var pubWG sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for i := 0; i < eventsEach; i++ {
				b.Publish(ev(JobEventScheduled, "concurrent"))
			}
		}()
	}
	pubWG.Wait()

	// Give subscribers a chance to drain.
	time.Sleep(200 * time.Millisecond)
	b.Close()
	wg.Wait()

	// Every subscriber should have received close to publishers * eventsEach,
	// modulo drops when the buffer overflows. Assert at least > 0 for each.
	for i, c := range received {
		if c.Load() == 0 {
			t.Errorf("subscriber %d received 0 events", i)
		}
	}
}

// TestJobEventBroker_NumSubscribers tracks Subscribe/cancel membership.
func TestJobEventBroker_NumSubscribers(t *testing.T) {
	b := NewJobEventBroker(4)
	if got := b.NumSubscribers(); got != 0 {
		t.Errorf("fresh broker NumSubscribers=%d, want 0", got)
	}
	_, c1 := b.Subscribe()
	_, c2 := b.Subscribe()
	if got := b.NumSubscribers(); got != 2 {
		t.Errorf("after two subscribes NumSubscribers=%d, want 2", got)
	}
	c1()
	if got := b.NumSubscribers(); got != 1 {
		t.Errorf("after one cancel NumSubscribers=%d, want 1", got)
	}
	c2()
	if got := b.NumSubscribers(); got != 0 {
		t.Errorf("after both cancels NumSubscribers=%d, want 0", got)
	}
}

// TestJobEventBroker_DefaultBuffer verifies zero/negative falls back to the
// sensible default.
func TestJobEventBroker_DefaultBuffer(t *testing.T) {
	for _, sz := range []int{0, -1, -100} {
		b := NewJobEventBroker(sz)
		sub, cancel := b.Subscribe()
		if cap(sub.ch) != 256 {
			t.Errorf("NewJobEventBroker(%d): buffer cap=%d, want 256", sz, cap(sub.ch))
		}
		cancel()
	}
}
