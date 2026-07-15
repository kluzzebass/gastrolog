// Package sparkline is a generic, domain-free bounded history of samples —
// the data structure behind every inline mini-chart in the system. It knows
// nothing about rates, bytes, queue depths, or any other producer: it holds
// the last N values of type T and hands them back oldest-to-newest. Callers
// choose T and the capacity; a rate series composes one over its per-tick
// rates, a gauge composes one over its per-tick levels, and both render
// through the same series of numbers.
package sparkline

// Numeric is the set of sample types a Sparkline can hold: any integer or
// floating-point kind, including named types whose underlying type is one of
// these (~).
type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

// Sparkline is a bounded FIFO of the most recent samples. When a capacity is
// set, the oldest sample is dropped as each new one arrives (a ring buffer);
// with no capacity it grows without bound. The zero value is not usable —
// construct with New.
type Sparkline[T Numeric] struct {
	buf   []T
	cap   int // <= 0 means unbounded
	head  int // index of the oldest sample (ring mode only)
	count int // number of live samples
}

// New returns a Sparkline that retains at most capacity samples. A capacity
// of zero or less imposes no limit — the sparkline grows with every Push and
// the caller owns the memory implications. The capacity is the caller's
// choice, not a system-wide constant.
func New[T Numeric](capacity int) *Sparkline[T] {
	s := &Sparkline[T]{cap: capacity}
	if capacity > 0 {
		s.buf = make([]T, capacity)
	}
	return s
}

// Push appends a sample. In bounded mode, once full, it overwrites the oldest
// sample rather than growing. O(1) in both modes.
func (s *Sparkline[T]) Push(v T) {
	if s.cap <= 0 {
		s.buf = append(s.buf, v)
		s.count++
		return
	}
	if s.count < s.cap {
		s.buf[(s.head+s.count)%s.cap] = v
		s.count++
		return
	}
	// Full: overwrite the oldest and advance the window.
	s.buf[s.head] = v
	s.head = (s.head + 1) % s.cap
}

// Values returns a fresh copy of the live samples, oldest first. The result
// is owned by the caller; mutating it does not affect the sparkline, and
// later Push calls do not affect a previously returned slice. Empty when no
// samples have been pushed.
func (s *Sparkline[T]) Values() []T {
	if s.count == 0 {
		return nil
	}
	out := make([]T, s.count)
	if s.cap <= 0 {
		copy(out, s.buf)
		return out
	}
	for i := range s.count {
		out[i] = s.buf[(s.head+i)%s.cap]
	}
	return out
}

// Last returns the most recently pushed sample and true, or the zero value
// and false when the sparkline is empty.
func (s *Sparkline[T]) Last() (T, bool) {
	if s.count == 0 {
		var zero T
		return zero, false
	}
	if s.cap <= 0 {
		return s.buf[len(s.buf)-1], true
	}
	return s.buf[(s.head+s.count-1)%s.cap], true
}

// Len is the number of live samples.
func (s *Sparkline[T]) Len() int { return s.count }

// Cap is the retention limit, or 0 when unbounded.
func (s *Sparkline[T]) Cap() int {
	if s.cap < 0 {
		return 0
	}
	return s.cap
}

// Reset discards all samples, keeping the configured capacity.
func (s *Sparkline[T]) Reset() {
	s.head = 0
	s.count = 0
	if s.cap <= 0 {
		s.buf = s.buf[:0]
	}
}
