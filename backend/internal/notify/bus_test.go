package notify

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBusEmitToSingleSubscriber covers the happy path: one subscriber
// receives a sequence of typed events in order, with monotonic versions
// starting at 1.
func TestBusEmitToSingleSubscriber(t *testing.T) {
	t.Parallel()
	b := NewBus[string](16)
	_, ch, v0 := b.Subscribe()
	if v0 != 0 {
		t.Errorf("expected initial version 0, got %d", v0)
	}

	b.Emit("a")
	b.Emit("b")
	b.Emit("c")

	want := []string{"a", "b", "c"}
	for i, w := range want {
		select {
		case got := <-ch:
			if got.Event != w {
				t.Errorf("event %d: got %q, want %q", i, got.Event, w)
			}
			if got.Version != uint64(i+1) {
				t.Errorf("event %d: got version %d, want %d", i, got.Version, i+1)
			}
		case <-time.After(time.Second):
			t.Fatalf("event %d: timed out waiting for delivery", i)
		}
	}
}

// TestBusFanout pins that every subscriber receives every event. The bus
// is the foundation for cluster-side stream broadcasting; if one slow
// subscriber blocked another, the fan-out would stall.
func TestBusFanout(t *testing.T) {
	t.Parallel()
	b := NewBus[int](16)
	const subs = 5
	const events = 10

	chans := make([]<-chan Versioned[int], subs)
	for i := range subs {
		_, ch, _ := b.Subscribe()
		chans[i] = ch
	}

	var wg sync.WaitGroup
	wg.Add(subs)
	var received atomic.Int64
	for _, ch := range chans {
		go func(c <-chan Versioned[int]) {
			defer wg.Done()
			for range events {
				<-c
				received.Add(1)
			}
		}(ch)
	}

	for i := range events {
		b.Emit(i)
	}
	wg.Wait()
	if got := received.Load(); got != int64(subs*events) {
		t.Errorf("expected %d total deliveries (%d subs × %d events), got %d", subs*events, subs, events, got)
	}
}

// TestBusDropsOnFullSubscriber asserts the contract that producers are
// never blocked by a slow subscriber: when a subscriber's channel fills,
// subsequent events are dropped silently and the producer continues. The
// subscriber must detect the drop by comparing the last received Version
// against the bus's current Version (trailing-edge gap).
func TestBusDropsOnFullSubscriber(t *testing.T) {
	t.Parallel()
	const bufSize = 4
	const emits = bufSize * 4
	b := NewBus[int](bufSize)
	_, ch, _ := b.Subscribe()

	// Emit more events than the channel can hold without anyone reading.
	for i := range emits {
		b.Emit(i)
	}

	// Drain everything the subscriber actually received.
	var got []uint64
	for {
		select {
		case msg := <-ch:
			got = append(got, msg.Version)
		default:
			goto done
		}
	}
done:
	// Bus version must reflect every emit, even the dropped ones — this
	// is what gives the subscriber a way to detect drops via comparing
	// last-received against bus.Version().
	if v := b.Version(); v != uint64(emits) {
		t.Errorf("bus version = %d, want %d (every emit must increment regardless of delivery)", v, emits)
	}
	// The subscriber should have received exactly the buffer worth — no
	// more (channel was never drained until end), no less (buffer wasn't
	// the bottleneck).
	if len(got) != bufSize {
		t.Errorf("received %d messages; want %d — buffer-bounded drop semantics broken", len(got), bufSize)
	}
	// Within the received set, Versions must be contiguous starting at 1:
	// the first bufSize emits filled the channel, the rest were dropped
	// at the send point. The "gap" is at the trailing edge between the
	// last received version and bus.Version().
	for i, v := range got {
		want := uint64(i + 1)
		if v != want {
			t.Errorf("received[%d].Version = %d, want %d", i, v, want)
		}
	}
	// Subscriber detects drops by noting last_received < bus.Version().
	if got[len(got)-1] >= b.Version() {
		t.Errorf("last received version %d should be strictly less than bus version %d so subscriber can detect the trailing gap",
			got[len(got)-1], b.Version())
	}
}

// TestBusUnsubscribeClosesChannel pins that Unsubscribe releases the
// subscriber's channel so range loops on the stream exit cleanly.
func TestBusUnsubscribeClosesChannel(t *testing.T) {
	t.Parallel()
	b := NewBus[string](4)
	id, ch, _ := b.Subscribe()
	b.Emit("first")
	<-ch // consume it
	b.Unsubscribe(id)
	if _, open := <-ch; open {
		t.Error("expected channel to be closed after Unsubscribe")
	}
}

// TestBusUnsubscribeUnknownIDIsNoop covers the contract that double-close
// is safe. Important because RPC handlers often defer Unsubscribe in a
// path where the bus may already have torn down the subscription on its
// own.
func TestBusUnsubscribeUnknownIDIsNoop(t *testing.T) {
	t.Parallel()
	b := NewBus[int](4)
	b.Unsubscribe(999) // no panic, no error
	id, _, _ := b.Subscribe()
	b.Unsubscribe(id)
	b.Unsubscribe(id) // idempotent
}

// TestBusSubscribeAfterEmitReturnsCurrentVersion verifies the contract
// that Subscribe returns the high-watermark at subscription time, so
// late-joining subscribers know their starting baseline and the first
// post-subscribe event will be Version + 1.
func TestBusSubscribeAfterEmitReturnsCurrentVersion(t *testing.T) {
	t.Parallel()
	b := NewBus[int](4)
	b.Emit(1)
	b.Emit(2)
	b.Emit(3)
	_, ch, v := b.Subscribe()
	if v != 3 {
		t.Errorf("subscribe-time version = %d, want 3", v)
	}
	b.Emit(99)
	got := <-ch
	if got.Version != 4 {
		t.Errorf("post-subscribe version = %d, want 4 (one past the watermark)", got.Version)
	}
	if got.Event != 99 {
		t.Errorf("post-subscribe event = %d, want 99", got.Event)
	}
}
