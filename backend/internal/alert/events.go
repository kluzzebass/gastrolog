package alert

// Event journal (gastrolog-1m3e0d): EEMUA 191 principle 8 separates alarms
// (conditions requiring operator ACTION) from events (records of OCCURRENCE)
// from metrics (health/trend). The alarm list must stay readable — history
// in it is exactly what makes alarm lists unreadable — so every lifecycle
// transition and demoted event-shaped diagnostic goes here instead: a
// per-node bounded ring of recent occurrences, served cluster-wide by the
// ListEvents RPC and rendered on its own quiet page.
//
// Restart semantics — decided, not implicit: the journal is IN-MEMORY ONLY
// and does NOT survive node restart. It is a ring of recent occurrences,
// not durable history (the durable records are the alarm lifecycle journal
// for ack/shelve state and the log stream for diagnostics). So that an
// empty journal after restart can never read as "nothing ever happened",
// every journal begins life with a node-started event carrying the boot
// instant — the journal's own birth is its first entry, and consumers
// render everything before it as unknown, not absent.
//
// The ring is bounded (DefaultEventJournalCapacity) and drops the OLDEST
// entry on overflow. Sequence numbers are per-node monotonic and keep
// counting across drops, so a reader can detect that entries have aged out
// (first visible seq > 1).

import (
	"sync"
	"time"
)

// Event types. Alarm lifecycle transitions are emitted by the Collector —
// exactly one entry per transition edge; see the emission sites in
// collector.go. The exact set:
//
//   - alarm-raised: the alarm annunciated — the condition outlived its
//     delay-on window (immediately for zero-delay types). A new occurrence
//     on a retained cleared-unacknowledged entry raises again.
//   - alarm-cleared: the condition resolved — the alarm released, latched
//     standing, or was retained cleared-unacknowledged (the detail says
//     which). For delay-off types this is the window-close edge.
//   - alarm-acked: operator acknowledgment (carries By). Also emitted when
//     journal replay re-applies a pre-restart ack to the first
//     annunciation after boot — the detail says so.
//   - alarm-shelved / alarm-unshelved: operator shelve and early unshelve
//     (carry By).
//   - alarm-shelve-expired: a shelve lapsed on its own — the alarm
//     returned to the active list without operator involvement.
//
// Demoted diagnostics (phase-1 razor demotions that stayed event-shaped)
// and the node-started journal seed use the remaining types.
const (
	EventAlarmRaised        = "alarm-raised"
	EventAlarmCleared       = "alarm-cleared"
	EventAlarmAcked         = "alarm-acked"
	EventAlarmShelved       = "alarm-shelved"
	EventAlarmUnshelved     = "alarm-unshelved"
	EventAlarmShelveExpired = "alarm-shelve-expired"

	// EventNodeStarted seeds every journal at creation: the journal begins
	// at node start, and everything earlier is unknown, not absent.
	EventNodeStarted = "node-started"

	// Demoted diagnostics (gastrolog-29380r phase 1): event-shaped
	// transition edges that are not alarms — no operator action — but are
	// records of occurrence worth keeping beside the alarm history.
	EventElectionStorm   = "election-storm"
	EventWALLatency      = "raft-wal-latency"
	EventChannelPressure = "channel-pressure"
)

// DefaultEventJournalCapacity bounds the per-node ring. ~10k entries keeps
// hours-to-days of history through any realistic upset while holding a few
// megabytes at most.
const DefaultEventJournalCapacity = 10_000

// Event is one journal entry: a record of something that happened. No
// priority, no response text, no lifecycle — events require nothing of the
// operator.
type Event struct {
	// Seq is the per-node monotonic sequence, stamped by the journal on
	// Record. It keeps counting across ring drops.
	Seq  uint64
	Time time.Time
	// Type is one of the Event* constants above.
	Type string
	// Source is the component the event is about (an alarm's catalog
	// Source, "raft", "ingest-pipeline", "node", ...).
	Source string
	// AlarmID is the full alarm ID ("<type>" or "<type>:<instance>") for
	// alarm lifecycle events; empty otherwise.
	AlarmID string
	// Detail carries the human-readable specifics.
	Detail string
	// By is the operator identity on ack/shelve/unshelve events.
	By string
}

// EventJournal is a thread-safe bounded ring of recent events. Full ring
// drops the oldest entry per Record. In-memory only — see the package
// comment for the restart decision.
type EventJournal struct {
	mu    sync.Mutex
	buf   []Event // ring storage, allocated up front
	start int     // index of the oldest entry
	count int
	seq   uint64
	now   func() time.Time
}

// NewEventJournal creates a journal on the wall clock and seeds it with the
// node-started event — the journal's birth is its first entry, so an empty
// history after restart is visibly "journal begins here", never "nothing
// ever happened".
func NewEventJournal(capacity int) *EventJournal {
	return NewEventJournalWithClock(capacity, time.Now)
}

// NewEventJournalWithClock is NewEventJournal on an injectable clock, so
// tests stamp deterministic times. Sibling features on the collector share
// the same clock through it.
func NewEventJournalWithClock(capacity int, now func() time.Time) *EventJournal {
	if capacity < 1 {
		capacity = 1
	}
	j := &EventJournal{
		buf: make([]Event, capacity),
		now: now,
	}
	j.Record(Event{
		Type:   EventNodeStarted,
		Source: "node",
		Detail: "event journal begins — entries are held in memory and do not survive node restart",
	})
	return j
}

// Record appends one event, stamping Seq and — when the caller left it
// zero — Time. A full ring drops its oldest entry.
func (j *EventJournal) Record(e Event) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.seq++
	e.Seq = j.seq
	if e.Time.IsZero() {
		e.Time = j.now()
	}
	if j.count < len(j.buf) {
		j.buf[(j.start+j.count)%len(j.buf)] = e
		j.count++
		return
	}
	// Full: overwrite the oldest slot and advance the start.
	j.buf[j.start] = e
	j.start = (j.start + 1) % len(j.buf)
}

// Events returns a copy of the journal, oldest first.
func (j *EventJournal) Events() []Event {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Event, j.count)
	for i := range j.count {
		out[i] = j.buf[(j.start+i)%len(j.buf)]
	}
	return out
}

// Len returns the number of entries currently held.
func (j *EventJournal) Len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.count
}
