package notify

import "sync"

// Bus is a generic event broadcaster. Producers call Emit with a typed event;
// subscribers receive Versioned[T] envelopes on their channel in order. Each
// subscriber gets its own bounded channel — if a subscriber falls behind, its
// channel fills and subsequent events are dropped at the per-subscriber
// level. Subscribers detect drops via the monotonic Version field: a gap
// between successive Version values means events were lost and the subscriber
// should resync from authoritative state.
//
// Bus complements Signal: use Signal when consumers only need "something
// changed" (cheap wake-up), use Bus when consumers need the event content
// (chunk diffs, config diffs, etc.). The bare-signal pattern forced clients
// into expensive pull-after-push refetches under high event rates — Bus
// avoids that by carrying the change directly. See gastrolog-3pf9w.
//
// Bus is intentionally minimal: no replay buffer, no per-subscriber pull
// semantics, no built-in resync. Resync is the subscriber's responsibility
// — when it detects a version gap it should fetch authoritative state and
// then resume consuming the bus from the new high-watermark. This keeps the
// bus itself O(1) per emit regardless of subscriber count or lag.
type Bus[T any] struct {
	mu          sync.Mutex
	version     uint64
	subscribers map[uint64]chan Versioned[T]
	nextID      uint64
	bufSize     int
}

// Versioned wraps an event with the monotonic version number assigned at
// emit time. Subscribers track the last Version seen and compare against
// each received Version; an unexpected gap signals dropped events.
type Versioned[T any] struct {
	Version uint64
	Event   T
}

// NewBus creates a Bus with per-subscriber channels of bufSize events. A
// larger buffer tolerates more subscriber lag before drops occur; a smaller
// buffer surfaces drops sooner. Typical values: 256 for high-frequency
// streams (chunk progress), 64 for low-frequency streams (config commits).
// bufSize must be positive.
func NewBus[T any](bufSize int) *Bus[T] {
	if bufSize <= 0 {
		bufSize = 1
	}
	return &Bus[T]{bufSize: bufSize}
}

// Emit broadcasts an event to all current subscribers and returns the
// monotonic version assigned. Non-blocking: an event for a subscriber whose
// channel is full is dropped silently — the subscriber must detect via
// version gap and resync. Safe for concurrent producers.
func (b *Bus[T]) Emit(ev T) uint64 {
	b.mu.Lock()
	b.version++
	v := b.version
	msg := Versioned[T]{Version: v, Event: ev}
	// Snapshot subscriber channels under the lock so producers can't be
	// blocked by a subscriber that's in the middle of Unsubscribe.
	subs := make([]chan Versioned[T], 0, len(b.subscribers))
	for _, ch := range b.subscribers {
		subs = append(subs, ch)
	}
	b.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
			// Full — subscriber will detect via version gap on the next
			// successfully delivered event.
		}
	}
	return v
}

// Subscribe registers a new subscriber and returns a unique subscription
// ID, the receive channel, and the bus's current version. The caller must
// Unsubscribe when done to release the channel. The version returned is
// the high-watermark at subscription time; events delivered after this
// will carry Version > returned.
func (b *Bus[T]) Subscribe() (id uint64, ch <-chan Versioned[T], version uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subscribers == nil {
		b.subscribers = make(map[uint64]chan Versioned[T])
	}
	b.nextID++
	id = b.nextID
	out := make(chan Versioned[T], b.bufSize)
	b.subscribers[id] = out
	return id, out, b.version
}

// Unsubscribe removes a subscriber and closes its channel. Idempotent:
// calling with an unknown id is a no-op. Closes the channel exactly once.
func (b *Bus[T]) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subscribers[id]; ok {
		close(ch)
		delete(b.subscribers, id)
	}
}

// Version returns the current high-watermark — the version of the most
// recently emitted event, or 0 if none.
func (b *Bus[T]) Version() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.version
}
