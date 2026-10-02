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
//
// The broker also owns per-job event ORDERING: a one-time job starts running
// the moment it is created, so its Started/Completed/Failed can reach Publish
// before the scheduler's own Scheduled publish. Subscribers keying on "latest
// event wins" would then display a finished job as freshly queued, forever.
// ExpectScheduled, called before the job is created, makes the broker hold
// back that job's events until its Scheduled has been delivered.
type JobEventBroker struct {
	buffer int

	mu     sync.Mutex
	subs   map[*JobSubscription]struct{}
	closed bool
	// pending holds back events for job IDs whose Scheduled publish has been
	// announced (ExpectScheduled) but not yet delivered, in arrival order.
	pending map[string][]JobEvent
}

// NewJobEventBroker creates a broker with the given per-subscriber buffer
// size. Zero or negative falls back to a sensible default (256).
func NewJobEventBroker(buffer int) *JobEventBroker {
	if buffer <= 0 {
		buffer = 256
	}
	return &JobEventBroker{
		buffer:  buffer,
		subs:    make(map[*JobSubscription]struct{}),
		pending: make(map[string][]JobEvent),
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

// ExpectScheduled announces that a Scheduled event for jobID is on its way.
// Until it is published, every other event carrying that jobID is held back
// and replayed, in arrival order, right after the Scheduled delivery — the
// job may start (and finish) between its creation and the scheduler's
// Scheduled publish, and subscribers must never observe a job's lifecycle
// before learning it was scheduled. Call it BEFORE creating the job; a
// creation that fails must call AbandonScheduled with the same ID.
func (b *JobEventBroker) ExpectScheduled(jobID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || jobID == "" {
		return
	}
	if _, exists := b.pending[jobID]; !exists {
		b.pending[jobID] = nil
	}
}

// AbandonScheduled withdraws an ExpectScheduled announcement whose Scheduled
// will never be published (job creation failed). Any events held back under
// the ID are delivered rather than dropped — they are facts subscribers are
// owed even when the ordering promise cannot be kept.
func (b *JobEventBroker) AbandonScheduled(jobID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	queued, exists := b.pending[jobID]
	if !exists {
		return
	}
	delete(b.pending, jobID)
	for _, evt := range queued {
		b.deliverLocked(evt)
	}
}

// Publish delivers an event to every subscriber. Non-blocking: if a
// subscriber's buffer is full the event is dropped for that subscriber
// (the drop counter is incremented) but delivery to other subscribers
// continues. Safe to call concurrently with Subscribe and cancel.
//
// Delivery runs under b.mu so per-job ordering (ExpectScheduled) is a real
// guarantee rather than a race between publishers; sends are non-blocking,
// so the hold never waits on a subscriber.
func (b *JobEventBroker) Publish(evt JobEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	id := evt.Job.ID
	if queued, held := b.pending[id]; held {
		if evt.Kind != JobEventScheduled {
			b.pending[id] = append(queued, evt)
			return
		}
		delete(b.pending, id)
		b.deliverLocked(evt)
		for _, held := range queued {
			b.deliverLocked(held)
		}
		return
	}
	b.deliverLocked(evt)
}

// deliverLocked fans one event out to every subscriber. Caller holds b.mu.
func (b *JobEventBroker) deliverLocked(evt JobEvent) {
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
	b.pending = nil
	b.mu.Unlock()
	for sub := range subs {
		sub.close()
	}
}

// NumSubscribers returns the current subscriber count. Useful in tests
// and for diagnostics.
func (b *JobEventBroker) NumSubscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}
