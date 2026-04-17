package orchestrator

import (
	"sync"
	"sync/atomic"
)

// JobEventKind enumerates the job state transitions a subscriber can observe.
type JobEventKind int

const (
	// JobEventScheduled fires when a new one-time job is registered (RunOnce
	// or Submit). The job has not started executing yet.
	JobEventScheduled JobEventKind = iota + 1
	// JobEventStarted fires when a one-time job's wrapper has transitioned
	// the progress record to Running. Only produced by Submit-registered
	// jobs that carry a JobProgress — plain RunOnce jobs skip straight from
	// Scheduled to Completed/Failed.
	JobEventStarted
	// JobEventCompleted fires when a one-time job finishes successfully
	// (or when the job's task returns without an error, regardless of
	// progress-record status).
	JobEventCompleted
	// JobEventFailed fires when a one-time job returns an error. Job
	// failures still fire JobEventCompleted via gocron's AfterJobRuns
	// listener; JobEventFailed is reserved for Submit-registered jobs
	// whose progress record was marked failed.
	JobEventFailed
)

// String returns a short label for logs/metrics.
func (k JobEventKind) String() string {
	switch k {
	case JobEventScheduled:
		return "scheduled"
	case JobEventStarted:
		return "started"
	case JobEventCompleted:
		return "completed"
	case JobEventFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// JobEvent is a single observable transition on a scheduler job.
type JobEvent struct {
	Kind JobEventKind
	// Job is a snapshot of the job at the moment of the event. Snapshot so
	// subscribers see a stable view even if subsequent transitions update
	// the underlying JobInfo.
	Job JobInfo
}

// JobSubscription is a live subscription to job events. Callers read from
// Events() and call the cancel func returned by Subscribe() when done.
type JobSubscription struct {
	ch        chan JobEvent
	closeOnce sync.Once
	dropped   atomic.Int64
}

// close closes the subscription's channel exactly once. Safe for concurrent
// callers — used by both the cancel func and Broker.Close so whichever
// path fires first wins.
func (s *JobSubscription) close() {
	s.closeOnce.Do(func() { close(s.ch) })
}

// Events returns the subscriber's receive channel. Closed when the
// subscription is cancelled.
func (s *JobSubscription) Events() <-chan JobEvent { return s.ch }

// Dropped returns the number of events that have been dropped because the
// subscriber's buffer was full. Useful for diagnostics — a non-zero value
// means the subscriber isn't keeping up.
func (s *JobSubscription) Dropped() int64 { return s.dropped.Load() }

// JobEventBroker is a fan-out pub/sub for scheduler job transitions.
// Subscribers each get their own bounded channel; publish is non-blocking
// and drops events for slow subscribers rather than stalling the scheduler.
type JobEventBroker struct {
	buffer int

	mu     sync.RWMutex
	subs   map[*JobSubscription]struct{}
	closed bool
}

// NewJobEventBroker creates a broker with the given per-subscriber buffer
// size. Zero or negative falls back to a sensible default (256).
func NewJobEventBroker(buffer int) *JobEventBroker {
	if buffer <= 0 {
		buffer = 256
	}
	return &JobEventBroker{
		buffer: buffer,
		subs:   make(map[*JobSubscription]struct{}),
	}
}

// Subscribe registers a new subscriber. Returns the subscription and a
// cancel function that removes it and closes the channel. Safe to call
// cancel concurrently with publishes, with other cancels, and with
// Broker.Close.
func (b *JobEventBroker) Subscribe() (*JobSubscription, func()) {
	sub := &JobSubscription{ch: make(chan JobEvent, b.buffer)}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		sub.close()
		return sub, func() {}
	}
	b.subs[sub] = struct{}{}
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		delete(b.subs, sub)
		b.mu.Unlock()
		sub.close()
	}
	return sub, cancel
}

// Publish delivers an event to every subscriber. Non-blocking: if a
// subscriber's buffer is full the event is dropped for that subscriber
// (the drop counter is incremented) but delivery to other subscribers
// continues. Safe to call concurrently with Subscribe and cancel.
func (b *JobEventBroker) Publish(evt JobEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for sub := range b.subs {
		select {
		case sub.ch <- evt:
		default:
			sub.dropped.Add(1)
		}
	}
}

// Close permanently disables the broker and closes every subscriber's
// channel. Subsequent Subscribe/Publish calls are no-ops. Called on
// scheduler shutdown so subscribers (e.g. WatchJobs streams) see EOF.
func (b *JobEventBroker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	subs := b.subs
	b.subs = nil
	b.mu.Unlock()
	for sub := range subs {
		sub.close()
	}
}

// NumSubscribers returns the current subscriber count. Useful in tests
// and for diagnostics.
func (b *JobEventBroker) NumSubscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}
