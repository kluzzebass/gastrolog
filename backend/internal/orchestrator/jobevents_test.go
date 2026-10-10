package orchestrator

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// drainSubscription takes up to want events already queued on sub. Publish
// delivers to every subscriber's buffer before it returns, so whatever a
// returned Publish delivered is queued.
func drainSubscription(sub *JobSubscription, want int) []JobEvent {
	var got []JobEvent
	for len(got) < want {
		select {
		case evt, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, evt)
		default:
			return got
		}
	}
	return got
}

// requireClosed asserts sub's channel is already closed.
func requireClosed(t *testing.T, sub *JobSubscription, what string) {
	t.Helper()
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Errorf("%s: expected channel closed, got a value", what)
		}
	default:
		t.Fatalf("%s: channel not closed", what)
	}
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
	got := drainSubscription(sub, 1)
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
	gotA := drainSubscription(sa, 3)
	gotB := drainSubscription(sb, 3)
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

	// Fast subscriber still receives — drain what's in its buffer.
	gotFast := drainSubscription(fast, 4)
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

	requireClosed(t, sub, "after cancel")
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

	requireClosed(t, sub, "after broker.Close")

	// Publish after Close is a no-op.
	b.Publish(ev(JobEventScheduled, "post-close"))

	// Subscribe after Close returns an already-closed channel.
	late, cancelLate := b.Subscribe()
	defer cancelLate()
	requireClosed(t, late, "late subscription")
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
			for range sub.Events() {
				counter.Add(1)
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

	// Close lets each subscriber drain its buffer before it sees the channel
	// closed and exits.
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

// An announced job's own events must never be observable before its
// Scheduled: a one-time job starts the moment it is created, so Started and
// the terminal event can reach the broker first, and a subscriber keying on
// "latest event wins" would render a finished job as freshly queued forever.
// This drives that exact interleaving deterministically — no timing, no
// slower-CI dependency.
func TestJobEventBroker_HoldsEventsUntilScheduled(t *testing.T) {
	t.Parallel()
	b := NewJobEventBroker(8)
	sub, cancel := b.Subscribe()
	defer cancel()

	job := JobInfo{ID: "job-1", Name: "fast-failure"}
	b.ExpectScheduled("job-1")
	b.Publish(JobEvent{Kind: JobEventStarted, Job: job})
	b.Publish(JobEvent{Kind: JobEventFailed, Job: job})

	select {
	case evt := <-sub.Events():
		t.Fatalf("observed %v before the job's Scheduled was published", evt.Kind)
	default:
	}

	b.Publish(JobEvent{Kind: JobEventScheduled, Job: job})
	want := []JobEventKind{JobEventScheduled, JobEventStarted, JobEventFailed}
	for i, k := range want {
		evt := <-sub.Events()
		if evt.Kind != k {
			t.Fatalf("event[%d] kind=%v, want %v", i, evt.Kind, k)
		}
	}

	// The hold is gone: later events for the same ID stream straight through.
	b.Publish(JobEvent{Kind: JobEventCompleted, Job: job})
	if evt := <-sub.Events(); evt.Kind != JobEventCompleted {
		t.Fatalf("post-flush event kind=%v, want Completed", evt.Kind)
	}
}

// Unannounced jobs are untouched by the hold machinery.
func TestJobEventBroker_UnannouncedEventsPassThrough(t *testing.T) {
	t.Parallel()
	b := NewJobEventBroker(8)
	sub, cancel := b.Subscribe()
	defer cancel()

	b.Publish(JobEvent{Kind: JobEventFailed, Job: JobInfo{ID: "cron-ish"}})
	if evt := <-sub.Events(); evt.Kind != JobEventFailed {
		t.Fatalf("pass-through event kind=%v, want Failed", evt.Kind)
	}
}

// A withdrawn announcement (job creation failed) must deliver anything it
// held rather than swallow it — the events are facts subscribers are owed
// even when the ordering promise cannot be kept.
func TestJobEventBroker_AbandonDeliversHeldEvents(t *testing.T) {
	t.Parallel()
	b := NewJobEventBroker(8)
	sub, cancel := b.Subscribe()
	defer cancel()

	job := JobInfo{ID: "job-2", Name: "stillborn"}
	b.ExpectScheduled("job-2")
	b.Publish(JobEvent{Kind: JobEventFailed, Job: job})
	b.AbandonScheduled("job-2")

	if evt := <-sub.Events(); evt.Kind != JobEventFailed {
		t.Fatalf("abandoned hold delivered kind=%v, want Failed", evt.Kind)
	}
}
