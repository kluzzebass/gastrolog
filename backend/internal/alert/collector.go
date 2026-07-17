// Package alert provides a thread-safe registry of runtime alarms.
//
// An alarm is a condition requiring operator action, with documented cause
// and response (EEMUA 191 / ISA-18.2). Priority is a property of the alarm
// TYPE, derived from a consequence × urgency assessment recorded in the
// static catalog (registry.go) — call sites cannot choose it. Components
// raise alarms via Raise() when they detect a cataloged condition and clear
// them via Clear() when the condition resolves; the collector looks up the
// type and stamps priority, source, cause and response.
//
// Chattering suppression (EEMUA 191 principle 3) is also the collector's,
// driven by the catalog entry — call sites just Raise/Clear the raw
// condition:
//
//   - DelayOn: the condition must persist that long before the alarm
//     becomes active. A condition that flaps below the window never
//     annunciates.
//   - DelayOff: after an active alarm's condition clears, it must STAY
//     clear that long before the alarm auto-clears. A re-raise inside the
//     window is the same continuous occurrence, not a new one.
//   - Latching: the alarm stays active after the condition clears, until
//     operator acknowledgment. A latched alarm releases only when BOTH the
//     condition has resolved AND an operator has acked, in either order
//     (gastrolog-1z5gg4).
//
// Standing-alarm lifecycle (EEMUA 191 principles 5 & 6) is LAYERED on the
// suppression entry: the entry's conditionUp/active/latching substrate says
// what the condition is doing; the lifecycle fields (acked, shelvedUntil)
// say what the operator has done about it. The four visible states —
// active-unacked, active-acked, cleared-unacked, shelved — are derived from
// the combination on every read; see stateLocked and the combined state
// machine in docs/alarm-management-design.md. Ack and shelve survive node
// restart via a small journal under the node home (journal.go).
//
// Suppression windows are evaluated LAZILY: state advances on every
// Raise/Clear touching an alarm and on every read (Standing/Active/Count),
// against the collector's injectable clock. A condition raised once and
// never re-raised still activates once its DelayOn elapses — the next read
// surfaces it. Shelve expiry is settled the same lazy way — a time
// construct, never a timer. FirstSeen is the CONDITION start (the first
// Raise of the occurrence), not the moment the alarm activated: the
// delay-on window suppresses annunciation, not the condition's history.
//
// There is no path around the catalog: every alarm enters through Raise,
// and priority is never chosen at a call site.
package alert

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// Priority is the consequence × urgency verdict of an alarm type, recorded
// in the catalog. Critical means data loss is in progress or scheduled;
// High means durability or availability is degraded and will compound;
// Low needs attention on a human timescale.
type Priority int

const (
	Low      Priority = 1
	High     Priority = 2
	Critical Priority = 3
)

// String returns the operator-facing label for a priority.
func (p Priority) String() string {
	switch p {
	case Low:
		return "low"
	case High:
		return "high"
	case Critical:
		return "critical"
	default:
		return "unspecified"
	}
}

// AlarmState is the lifecycle state of a standing alarm, derived from the
// suppression substrate plus the operator's ack/shelve actions on every
// read. Mirrors the proto AlarmState enum.
type AlarmState int

const (
	// StateActiveUnacked: annunciated, not acknowledged. Also a latched
	// alarm whose condition resolved before acknowledgment — it stands.
	StateActiveUnacked AlarmState = 1
	// StateActiveAcked: condition still true, operator has acknowledged.
	StateActiveAcked AlarmState = 2
	// StateClearedUnacked: condition resolved while unacknowledged
	// (non-latching); retained so "it fired while you were away" stays
	// visible. Acknowledgment releases it.
	StateClearedUnacked AlarmState = 3
	// StateShelved: operator-shelved until an expiry. Never permanent.
	StateShelved AlarmState = 4
)

// String returns the operator-facing label for a lifecycle state.
func (s AlarmState) String() string {
	switch s {
	case StateActiveUnacked:
		return "active"
	case StateActiveAcked:
		return "acked"
	case StateClearedUnacked:
		return "cleared"
	case StateShelved:
		return "shelved"
	default:
		return "unspecified"
	}
}

// Lifecycle operation errors, mapped to RPC codes at the API boundary.
var (
	// ErrUnknownAlarm: no standing alarm with that ID (never annunciated,
	// or already released).
	ErrUnknownAlarm = errors.New("no standing alarm with that ID")
	// ErrShelveExpiryRequired: shelves are mandatory-expiry; a missing,
	// zero or negative duration is rejected at every boundary.
	ErrShelveExpiryRequired = errors.New("shelve requires a positive duration — shelves always expire")
	// ErrNotShelveable: the alarm's type refuses shelving (software
	// faults, alarm-flood — deferral is meaningless for them).
	ErrNotShelveable = errors.New("alarm type refuses shelving")
	// ErrAlarmCleared: shelve requested for an alarm whose condition has
	// already resolved; acknowledge it instead.
	ErrAlarmCleared = errors.New("alarm condition already cleared — acknowledge it instead of shelving")
	// ErrNotShelved: unshelve requested for an alarm that is not shelved.
	ErrNotShelved = errors.New("alarm is not shelved")
)

// Alarm is one standing alarm instance: a cataloged type plus the instance
// it fired for. ID is the stable dedup key, composed from the type ID and
// instance key.
type Alarm struct {
	ID            string // "<typeID>" or "<typeID>:<instanceKey>"
	TypeID        string
	InstanceKey   string
	Priority      Priority
	SoftwareFault bool   // outside the priority scale; see AlarmType
	Source        string // component name (e.g. "placement", "chunking")
	Detail        string // per-instance specifics from the raiser
	Cause         string // from the catalog: what condition this is
	Response      string // from the catalog: what the operator should do
	FirstSeen     time.Time
	LastSeen      time.Time

	// Lifecycle (gastrolog-1z5gg4). State is derived at snapshot time;
	// AckedBy/AckedAt are set once acknowledged; ShelvedUntil only while
	// shelved. Occurrences counts distinct condition occurrences (the
	// suppression sense: episodes separated by more than the delay-off
	// window) that have annunciated since the alarm became standing.
	State        AlarmState
	AckedBy      string
	AckedAt      time.Time
	ShelvedUntil time.Time
	Occurrences  int
	// Shelveable is the catalog verdict (AlarmType.Shelveable), carried on
	// the snapshot so consumers can suppress the shelve control entirely.
	Shelveable bool
}

// Sink is the raising side of the collector, satisfied by *Collector. It is
// THE alarm-sink interface — components that raise alarms depend on this one
// type rather than declaring structurally identical local copies.
type Sink interface {
	Raise(typeID, instanceKey, detail string)
	Clear(typeID, instanceKey string)
}

// entry is the collector's per-alarm suppression state machine plus the
// lifecycle layer on top of it. The alarm inside it is only visible through
// Standing()/Active() once the entry has activated.
type entry struct {
	alarm Alarm

	// Suppression parameters, stamped from the catalog at raise time.
	delayOn  time.Duration
	delayOff time.Duration
	latching bool

	// conditionUp is whether the raiser currently asserts the condition
	// (raised and not since cleared).
	conditionUp bool
	// conditionSince is when the current continuous condition occurrence
	// began — the DelayOn window measures from here. It is also the
	// alarm's FirstSeen.
	conditionSince time.Time
	// clearedAt is when the condition last went down while the alarm was
	// active — the DelayOff window measures from here.
	clearedAt time.Time
	// active is whether the alarm has annunciated: the condition outlived
	// DelayOn (immediately, for zero-delay types).
	active bool

	// ---- lifecycle layer (gastrolog-1z5gg4) ----

	// cleared marks a retained cleared-unacked entry: the condition
	// resolved (past delay-off) while unacknowledged, and the alarm is
	// kept visible until an operator acks it. A re-raise on a cleared
	// entry starts a NEW occurrence (delay-on applies again).
	cleared bool
	// acked records operator acknowledgment of the current occurrence.
	acked   bool
	ackedBy string
	ackedAt time.Time
	// shelvedUntil is the mandatory shelve expiry; zero when not shelved.
	// Expiry is settled lazily against the collector clock.
	shelvedUntil time.Time
	// occurrences counts annunciated condition occurrences (activation
	// edges) since this entry became standing.
	occurrences int
	// shelveable is the catalog verdict, stamped at raise time.
	shelveable bool
}

// shelved reports whether the entry is currently shelved at instant now.
func (e *entry) shelved(now time.Time) bool {
	return !e.shelvedUntil.IsZero() && now.Before(e.shelvedUntil)
}

// stateAt derives the lifecycle state of an annunciated entry.
func (e *entry) stateAt(now time.Time) AlarmState {
	switch {
	case e.shelved(now):
		return StateShelved
	case e.cleared:
		return StateClearedUnacked
	case e.acked:
		return StateActiveAcked
	default:
		// Includes a latched alarm whose condition resolved: it stands
		// active-unacked until acknowledged.
		return StateActiveUnacked
	}
}

// Collector is a thread-safe, in-process registry of active alarms with
// catalog-driven chattering suppression.
type Collector struct {
	mu      sync.Mutex
	entries map[string]*entry

	// now is the collector's clock. Injectable (NewWithClock) so
	// suppression tests advance time deterministically — never with
	// sleeps — and so sibling features on the collector share one clock.
	now func() time.Time

	// onActivate, when set, is invoked after an alarm annunciates — the
	// inactive → active transition in settleLocked, which for delayed
	// types may happen on a read rather than a Raise. Never invoked on a
	// refresh of an already-active alarm or a delay-off resume of the
	// same occurrence. Called outside the collector lock, because the
	// hook may raise back into the collector (the rate monitor raising
	// alarm-flood). Wired to RateMonitor.Observe.
	onActivate func(typeID string)

	// journal persists ack/shelve state across restart (journal.go); nil
	// when no journal is attached (tests, in-memory setups). pending holds
	// replayed lifecycle state waiting for its alarm ID to annunciate
	// again after startup; consumed on the activation edge.
	journal *journal
	pending map[string]pendingLifecycle

	// onEvent, when set, receives EXACTLY ONE Event per alarm lifecycle
	// transition edge (gastrolog-1m3e0d; see the Event* constants in
	// events.go for the set). Same discipline as onActivate: transition
	// edges are collected under the lock into pendingEvents and emitted
	// after unlocking, so the hook may safely touch the collector. Wired
	// to EventJournal.Record.
	onEvent func(Event)
	// pendingEvents accumulates transition events under c.mu until the
	// public method that took the lock drains and emits them.
	pendingEvents []Event
}

// SetOnActivate installs the activation hook. Wire once at startup, before
// components start raising.
func (c *Collector) SetOnActivate(fn func(typeID string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onActivate = fn
}

// fireActivations invokes the activation hook for each annunciated type.
// Callers pass the hook captured under the lock and call this after
// unlocking — hook re-entry into the collector must not deadlock.
func fireActivations(hook func(typeID string), typeIDs []string) {
	if hook == nil {
		return
	}
	for _, t := range typeIDs {
		hook(t)
	}
}

// SetOnEvent installs the lifecycle-transition event hook (the event
// journal). Wire once at startup, before components start raising.
func (c *Collector) SetOnEvent(fn func(Event)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onEvent = fn
}

// eventLocked queues one lifecycle-transition event, stamped with the
// collector clock — lazily settled transitions record the instant the
// settling read ran, same as their slog lines. No-op without a hook.
// Caller holds c.mu.
func (c *Collector) eventLocked(e Event) {
	if c.onEvent == nil {
		return
	}
	e.Time = c.now()
	c.pendingEvents = append(c.pendingEvents, e)
}

// drainEventsLocked hands the queued transition events to the caller for
// emission after unlock. Caller holds c.mu.
func (c *Collector) drainEventsLocked() (hook func(Event), events []Event) {
	events = c.pendingEvents
	c.pendingEvents = nil
	return c.onEvent, events
}

// emitEvents delivers drained transition events outside the collector lock,
// mirroring fireActivations.
func emitEvents(hook func(Event), events []Event) {
	if hook == nil {
		return
	}
	for _, e := range events {
		hook(e)
	}
}

// New creates a new alarm collector on the wall clock.
func New() *Collector {
	return NewWithClock(time.Now)
}

// NewWithClock creates an alarm collector whose suppression windows are
// measured against the given clock. Tests advance it deterministically.
func NewWithClock(now func() time.Time) *Collector {
	return &Collector{
		entries: make(map[string]*entry),
		now:     now,
	}
}

// alarmID composes the stable dedup key for a type + instance pair.
// Node-scoped types have no instance key and use the bare type ID.
func alarmID(typeID, instanceKey string) string {
	if instanceKey == "" {
		return typeID
	}
	return typeID + ":" + instanceKey
}

// Raise reports that the condition of the given cataloged type holds for
// the given instance. The collector stamps priority, source, cause and
// response from the catalog and applies the type's suppression: with a
// DelayOn the alarm activates only once the condition has persisted that
// long (re-raises refresh detail but do not restart the window; a Clear
// does). detail carries the per-instance specifics.
//
// Raising an unregistered type is a catalog defect in the raising component.
// It is loud, never silent: the defect is logged AND the condition still
// surfaces — immediately, as a software-fault alarm with the raiser's
// detail, so neither the underlying condition nor the defect can disappear.
func (c *Collector) Raise(typeID, instanceKey, detail string) {
	t, ok := TypeByID(typeID)
	if !ok {
		slog.Error("alarm raised for a type missing from the alarm catalog — software defect in the raising component",
			"type", typeID, "instance", instanceKey, "detail", detail)
		t = unregisteredAlarmType(typeID)
	}
	c.raise(Alarm{
		ID:            alarmID(typeID, instanceKey),
		TypeID:        typeID,
		InstanceKey:   instanceKey,
		Priority:      t.Priority,
		SoftwareFault: t.SoftwareFault,
		Source:        t.Source,
		Detail:        detail,
		Cause:         t.Cause,
		Response:      t.Response,
		Shelveable:    t.Shelveable(),
	}, t.DelayOn, t.DelayOff, t.Latching)
}

// raise records that the condition for a is up, creating or refreshing its
// suppression entry. Detail refreshes on every raise (the cataloged fields
// are static but harmlessly re-stamped); FirstSeen is preserved across the
// occurrence.
func (c *Collector) raise(a Alarm, delayOn, delayOff time.Duration, latching bool) {
	c.mu.Lock()
	var activated []string

	now := c.now()
	e := c.entries[a.ID]
	if e != nil && c.settleLocked(e, now, &activated) {
		// The previous occurrence had fully released (e.g. acked and its
		// delay-off window expired); this raise starts a fresh entry.
		c.removeLocked(a.ID)
		e = nil
	}
	if e == nil {
		e = &entry{
			delayOn:        delayOn,
			delayOff:       delayOff,
			latching:       latching,
			conditionUp:    true,
			conditionSince: now,
			shelveable:     a.Shelveable,
		}
		a.FirstSeen = now
		a.LastSeen = now
		e.alarm = a
		c.entries[a.ID] = e
		c.settleLocked(e, now, &activated) // zero DelayOn activates immediately
	} else {
		firstSeen := e.alarm.FirstSeen
		e.alarm = a
		e.alarm.FirstSeen = firstSeen
		e.alarm.LastSeen = now
		e.delayOn, e.delayOff, e.latching = delayOn, delayOff, latching
		e.shelveable = a.Shelveable
		switch {
		case e.cleared && !e.conditionUp:
			// The condition returned on a retained cleared-unacked entry:
			// a NEW occurrence of the same alarm ID. Delay-on applies
			// again — the entry keeps showing cleared-unacked while the
			// new occurrence sits inside its window, and the activation
			// edge (settleLocked) promotes it: occurrence count up, ack
			// reset, FirstSeen reset to the new condition start.
			e.conditionUp = true
			e.conditionSince = now
			e.clearedAt = time.Time{}
		case !e.cleared && !e.conditionUp:
			// The condition returned inside the delay-off window (or on a
			// latched alarm): the same occurrence continues — the alarm stays
			// active with its FirstSeen, no re-occurrence, no activation edge.
			e.conditionUp = true
			e.clearedAt = time.Time{}
		}
		c.settleLocked(e, now, &activated)
	}
	hook := c.onActivate
	evHook, events := c.drainEventsLocked()
	c.mu.Unlock()
	fireActivations(hook, activated)
	emitEvents(evHook, events)
}

// Clear reports that the condition of the given type for the given instance
// no longer holds. No-op if nothing is tracked for it. What happens next is
// the type's suppression + lifecycle verdict:
//
//   - never activated (condition died inside DelayOn): dropped silently —
//     that is the chattering the window exists to suppress.
//   - active, Latching: stays standing until acknowledged. If the operator
//     already acked, the latch is satisfied and the alarm releases now.
//   - active, non-latching (once DelayOff has run, immediately for zero):
//     acked or shelved → released (the operator has already handled it);
//     unacked → retained as cleared-unacked until acknowledged.
//   - active, DelayOff set: stays active until the condition has stayed
//     clear for the whole window; a Raise inside it resumes the same
//     occurrence.
func (c *Collector) Clear(typeID, instanceKey string) {
	c.mu.Lock()
	var activated []string
	c.clearLocked(typeID, instanceKey, &activated)
	hook := c.onActivate
	evHook, events := c.drainEventsLocked()
	c.mu.Unlock()
	// A pending condition whose delay-on elapsed before this Clear arrived
	// annunciated during settling — it outlived its window, so it counts.
	fireActivations(hook, activated)
	emitEvents(evHook, events)
}

// clearLocked is Clear's body; caller holds c.mu.
func (c *Collector) clearLocked(typeID, instanceKey string, activated *[]string) {
	id := alarmID(typeID, instanceKey)
	e, ok := c.entries[id]
	if !ok {
		return
	}
	now := c.now()
	if c.settleLocked(e, now, activated) {
		c.removeLocked(id)
		return
	}
	if !e.active {
		// The condition never outlived its delay-on window: suppressed.
		c.removeLocked(id)
		return
	}
	if !e.conditionUp {
		// Repeat clear inside the delay-off window: the window keeps
		// measuring from the FIRST clear — the condition never came back.
		return
	}
	e.conditionUp = false
	e.clearedAt = now
	if e.latching {
		if e.acked {
			// The latch is satisfied: condition resolved AND acknowledged
			// (in this order, ack first). Release.
			slog.Info("latched alarm released — condition resolved after acknowledgment",
				"id", e.alarm.ID, "source", e.alarm.Source, "acked_by", e.ackedBy)
			c.eventLocked(Event{
				Type:    EventAlarmCleared,
				Source:  e.alarm.Source,
				AlarmID: e.alarm.ID,
				Detail:  "condition resolved after acknowledgment — released",
			})
			c.removeLocked(id)
			return
		}
		slog.Info("alarm condition cleared but the alarm is latched — standing until operator acknowledgment",
			"id", e.alarm.ID, "source", e.alarm.Source)
		c.eventLocked(Event{
			Type:    EventAlarmCleared,
			Source:  e.alarm.Source,
			AlarmID: e.alarm.ID,
			Detail:  "condition resolved — latched, standing until acknowledged",
		})
		return
	}
	if e.delayOff <= 0 {
		c.conditionResolvedLocked(id, e, now)
	}
}

// conditionResolvedLocked handles a non-latching entry whose condition has
// fully resolved (delay-off elapsed, or zero): acked or shelved entries
// release — the operator has already handled them — and unacked entries are
// retained as cleared-unacked so "it fired while you were away" stays
// visible until acknowledged. Caller holds c.mu.
func (c *Collector) conditionResolvedLocked(id string, e *entry, now time.Time) {
	if e.acked || e.shelved(now) {
		if e.delayOn > 0 || e.acked {
			slog.Info("alarm cleared — condition resolved",
				"id", e.alarm.ID, "source", e.alarm.Source)
		}
		c.eventLocked(Event{
			Type:    EventAlarmCleared,
			Source:  e.alarm.Source,
			AlarmID: e.alarm.ID,
			Detail:  "condition resolved — released",
		})
		c.removeLocked(id)
		return
	}
	if !e.cleared {
		e.cleared = true
		// A cleared alarm cannot stay shelved (there is nothing standing to
		// suppress); drop any expired shelve residue.
		e.shelvedUntil = time.Time{}
		slog.Info("alarm cleared — condition resolved; retained until acknowledged",
			"id", e.alarm.ID, "source", e.alarm.Source)
		c.eventLocked(Event{
			Type:    EventAlarmCleared,
			Source:  e.alarm.Source,
			AlarmID: e.alarm.ID,
			Detail:  "condition resolved — retained until acknowledged",
		})
	}
}

// settleLocked advances one entry's suppression + lifecycle state to now:
// it expires a lapsed shelve, activates a pending entry whose condition has
// outlived DelayOn, and resolves an active entry whose condition has stayed
// clear past DelayOff — releasing it (report true so the caller removes it)
// when acked or shelved, retaining it as cleared-unacked otherwise.
// Transition edges of suppressed types are logged here — the call site
// cannot log them, since it no longer knows when the window elapses.
// Annunciations are appended to activated for the caller to fire through
// the activation hook after unlocking. Caller holds c.mu.
func (c *Collector) settleLocked(e *entry, now time.Time, activated *[]string) (expired bool) {
	// Shelve expiry is a lazy time construct like the delay windows. A
	// shelve that lapses returns the alarm to active-unacked: the deferral
	// window the operator chose is over, so the condition demands fresh
	// attention — any acknowledgment is reset along with the shelve. The
	// pre-expiry value is kept for the resolution decision below, which
	// must be evaluated at the instant the delay-off window closed, not at
	// whatever instant this read happens to run.
	shelvedBefore := e.shelvedUntil
	if !e.shelvedUntil.IsZero() && !now.Before(e.shelvedUntil) {
		e.shelvedUntil = time.Time{}
		e.acked, e.ackedBy, e.ackedAt = false, "", time.Time{}
		slog.Info("alarm shelve expired — returned to the active list",
			"id", e.alarm.ID, "source", e.alarm.Source)
		c.eventLocked(Event{
			Type:    EventAlarmShelveExpired,
			Source:  e.alarm.Source,
			AlarmID: e.alarm.ID,
			Detail:  "shelve expired — returned to the active list",
		})
	}
	if e.conditionUp {
		// Activation covers two shapes: the first annunciation of a fresh
		// entry (!active) and a NEW occurrence on a retained cleared-unacked
		// entry (active && cleared) — both run the full delay-on window.
		if (!e.active || e.cleared) && now.Sub(e.conditionSince) >= e.delayOn {
			c.activateLocked(e, activated)
		}
		return false
	}
	if e.active && !e.cleared && !e.latching && now.Sub(e.clearedAt) >= e.delayOff {
		// The condition resolved when its delay-off window closed. The
		// lifecycle verdict is taken at THAT instant — lazy settling must
		// reach the same state no matter when the next read runs: shelved
		// or acked at resolution → released; unacked → cleared-unacked.
		resolvedAt := e.clearedAt.Add(e.delayOff)
		wasShelved := !shelvedBefore.IsZero() && resolvedAt.Before(shelvedBefore)
		if e.acked || wasShelved {
			if e.delayOff > 0 {
				slog.Info("alarm cleared — condition stayed clear past its delay-off window",
					"id", e.alarm.ID, "source", e.alarm.Source, "delay_off", e.delayOff)
			}
			c.eventLocked(Event{
				Type:    EventAlarmCleared,
				Source:  e.alarm.Source,
				AlarmID: e.alarm.ID,
				Detail:  "condition stayed clear past its delay-off window — released",
			})
			return true
		}
		e.cleared = true
		e.shelvedUntil = time.Time{}
		slog.Info("alarm cleared — condition resolved; retained until acknowledged",
			"id", e.alarm.ID, "source", e.alarm.Source)
		c.eventLocked(Event{
			Type:    EventAlarmCleared,
			Source:  e.alarm.Source,
			AlarmID: e.alarm.ID,
			Detail:  "condition stayed clear past its delay-off window — retained until acknowledged",
		})
	}
	return false
}

// activateLocked annunciates an entry whose condition has outlived its
// delay-on window: the first annunciation of a fresh entry, or a new
// occurrence on a retained cleared-unacked entry. Caller holds c.mu.
func (c *Collector) activateLocked(e *entry, activated *[]string) {
	if e.cleared {
		// New occurrence: the previous one's retention (and any
		// acknowledgment — there was none, it was cleared-UNACKED) ends.
		e.cleared = false
		e.acked, e.ackedBy, e.ackedAt = false, "", time.Time{}
	}
	e.active = true
	// FirstSeen is the current occurrence's condition start — for a fresh
	// entry this is already true; for a re-occurrence it resets (the
	// occurrence counter keeps the history).
	e.alarm.FirstSeen = e.conditionSince
	e.occurrences++
	c.eventLocked(Event{
		Type:    EventAlarmRaised,
		Source:  e.alarm.Source,
		AlarmID: e.alarm.ID,
		Detail:  e.alarm.Detail,
	})
	c.applyPendingLocked(e)
	*activated = append(*activated, e.alarm.TypeID)
	if e.delayOn > 0 {
		slog.Warn("alarm active — condition persisted past its delay-on window",
			"id", e.alarm.ID, "source", e.alarm.Source, "delay_on", e.delayOn, "detail", e.alarm.Detail)
	}
}

// applyPendingLocked applies journal-replayed lifecycle state to an entry on
// its first annunciation after startup. An expired replayed shelve applies
// nothing (mirrors live expiry, which also resets acknowledgment). Caller
// holds c.mu.
func (c *Collector) applyPendingLocked(e *entry) {
	p, ok := c.pending[e.alarm.ID]
	if !ok {
		return
	}
	delete(c.pending, e.alarm.ID)
	now := c.now()
	if !p.ShelvedUntil.IsZero() && !now.Before(p.ShelvedUntil) {
		// Shelve lapsed while the node was down (or before the condition
		// returned): active-unacked, nothing to re-apply.
		return
	}
	e.acked, e.ackedBy, e.ackedAt = p.Acked, p.AckedBy, p.AckedAt
	e.shelvedUntil = p.ShelvedUntil
	if p.Acked || !p.ShelvedUntil.IsZero() {
		slog.Info("alarm lifecycle state replayed from journal",
			"id", e.alarm.ID, "acked", p.Acked, "shelved_until", p.ShelvedUntil)
		// One event per re-application: the alarm's visible state changed
		// on this node. Shelve wins the label when both are set (a live
		// shelve resets acknowledgment; a folded ack-while-shelved keeps
		// both, and shelved is the state the operator sees).
		ev := Event{
			Type:    EventAlarmAcked,
			Source:  e.alarm.Source,
			AlarmID: e.alarm.ID,
			By:      p.AckedBy,
			Detail:  "acknowledgment replayed from the alarm lifecycle journal after restart",
		}
		if !p.ShelvedUntil.IsZero() {
			ev.Type = EventAlarmShelved
			ev.By = ""
			ev.Detail = "shelve replayed from the alarm lifecycle journal after restart — until " +
				p.ShelvedUntil.UTC().Format(time.RFC3339)
		}
		c.eventLocked(ev)
	}
}

// removeLocked releases an entry entirely and prunes any journal state for
// its ID — the alarm is gone, so a replay after restart must not resurrect
// operator actions against a future occurrence. Caller holds c.mu.
func (c *Collector) removeLocked(id string) {
	e := c.entries[id]
	delete(c.entries, id)
	if c.journal == nil {
		return
	}
	if _, hadPending := c.pending[id]; hadPending || (e != nil && (e.acked || !e.shelvedUntil.IsZero())) {
		delete(c.pending, id)
		c.journal.append(journalRecord{Op: journalOpResolve, ID: id, At: c.now()})
	}
}

// snapshot walks all entries under the lock, settles their suppression +
// lifecycle state, and returns copies of the annunciated ones that pass
// keep. Sorted by FirstSeen.
func (c *Collector) snapshot(keep func(AlarmState) bool) []*Alarm {
	c.mu.Lock()
	var activated []string

	now := c.now()
	var result []*Alarm
	for id, e := range c.entries {
		if c.settleLocked(e, now, &activated) {
			c.removeLocked(id)
			continue
		}
		if !e.active {
			continue
		}
		st := e.stateAt(now)
		if !keep(st) {
			continue
		}
		cp := e.alarm
		cp.State = st
		cp.AckedBy, cp.AckedAt = e.ackedBy, e.ackedAt
		if st == StateShelved {
			cp.ShelvedUntil = e.shelvedUntil
		}
		cp.Occurrences = e.occurrences
		cp.Shelveable = e.shelveable
		result = append(result, &cp)
	}
	hook := c.onActivate
	evHook, events := c.drainEventsLocked()
	c.mu.Unlock()
	// Delayed conditions may annunciate on a read — the rate monitor must
	// still see them.
	fireActivations(hook, activated)
	emitEvents(evHook, events)

	if len(result) == 0 {
		return nil
	}
	slices.SortFunc(result, func(a, b *Alarm) int {
		return a.FirstSeen.Compare(b.FirstSeen)
	})
	return result
}

// isActiveState reports whether a lifecycle state demands a place in the
// active list: the condition is standing (or latched standing) and not
// operator-suppressed.
func isActiveState(s AlarmState) bool {
	return s == StateActiveUnacked || s == StateActiveAcked
}

// Standing returns a snapshot of every visible alarm — active (unacked and
// acked), cleared-unacked, and shelved — each stamped with its lifecycle
// state, sorted by FirstSeen. This is the broadcast surface: alarms in
// every state travel in NodeStats so any node can serve ack/shelve for
// them. Reading settles suppression and lifecycle state lazily.
func (c *Collector) Standing() []*Alarm {
	return c.snapshot(func(AlarmState) bool { return true })
}

// Active returns a snapshot of the alarms in the active states only —
// pending delay-on conditions, shelved alarms and retained cleared-unacked
// alarms are excluded — sorted by FirstSeen.
func (c *Collector) Active() []*Alarm {
	return c.snapshot(isActiveState)
}

// Count returns the number of alarms in the active states (the same set
// Active returns), settling state like Active.
func (c *Collector) Count() int {
	return len(c.Active())
}

// HasStanding reports whether an alarm with the given full ID is currently
// visible in any lifecycle state on this collector.
func (c *Collector) HasStanding(id string) bool {
	for _, a := range c.Standing() {
		if a.ID == id {
			return true
		}
	}
	return false
}

// visibleLocked settles e and returns whether it is annunciated (visible in
// some lifecycle state). Caller holds c.mu; removal on release is applied.
func (c *Collector) visibleLocked(id string, e *entry, activated *[]string) bool {
	if c.settleLocked(e, c.now(), activated) {
		c.removeLocked(id)
		return false
	}
	return e.active
}

// Ack acknowledges the standing alarm with the given full ID, recording
// operator awareness (who + when):
//
//   - active, condition standing: → active-acked. The alarm then releases
//     silently when the condition resolves.
//   - cleared-unacked: released now — the ack is what it was waiting for.
//   - latched with the condition already resolved: released now (the latch
//     needs both resolution and ack, in either order).
//   - already acked: idempotent; who/when refresh.
//
// Returns ErrUnknownAlarm if no standing alarm has that ID.
func (c *Collector) Ack(id, by string) error {
	c.mu.Lock()
	var activated []string
	err := c.ackLocked(id, by, &activated)
	hook := c.onActivate
	evHook, events := c.drainEventsLocked()
	c.mu.Unlock()
	fireActivations(hook, activated)
	emitEvents(evHook, events)
	return err
}

func (c *Collector) ackLocked(id, by string, activated *[]string) error {
	e, ok := c.entries[id]
	if !ok || !c.visibleLocked(id, e, activated) {
		return ErrUnknownAlarm
	}
	now := c.now()
	if e.cleared && e.conditionUp {
		// Cleared-unacked with the condition already back and pending its
		// delay-on window: the ack releases the retention, and the pending
		// occurrence keeps tracking — it annunciates as a fresh alarm if
		// it outlives the window.
		slog.Info("alarm acknowledged and released — new occurrence pending its delay-on window",
			"id", id, "acked_by", by)
		c.eventLocked(Event{
			Type:    EventAlarmAcked,
			Source:  e.alarm.Source,
			AlarmID: id,
			By:      by,
			Detail:  "acknowledged and released — new occurrence pending its delay-on window",
		})
		e.cleared = false
		e.active = false
		return nil
	}
	if e.cleared || (e.latching && !e.conditionUp) {
		// The condition has already resolved; acknowledgment is the one
		// thing the alarm was standing for. Release it.
		slog.Info("alarm acknowledged and released — condition already resolved",
			"id", id, "acked_by", by)
		c.eventLocked(Event{
			Type:    EventAlarmAcked,
			Source:  e.alarm.Source,
			AlarmID: id,
			By:      by,
			Detail:  "acknowledged and released — condition already resolved",
		})
		c.removeLocked(id)
		return nil
	}
	e.acked, e.ackedBy, e.ackedAt = true, by, now
	slog.Info("alarm acknowledged", "id", id, "acked_by", by)
	c.eventLocked(Event{
		Type:    EventAlarmAcked,
		Source:  e.alarm.Source,
		AlarmID: id,
		By:      by,
		Detail:  "acknowledged",
	})
	c.journalAppendLocked(journalRecord{Op: journalOpAck, ID: id, By: by, At: now})
	return nil
}

// Shelve suppresses the standing alarm with the given full ID until now+d.
// The expiry is mandatory (d must be positive — there are no permanent
// shelves) and the alarm's type must allow shelving. Shelving resets any
// acknowledgment: when the shelve lapses with the condition still true, the
// alarm returns to active-unacked and demands fresh attention. Returns the
// expiry instant.
func (c *Collector) Shelve(id string, d time.Duration, by string) (time.Time, error) {
	if d <= 0 {
		return time.Time{}, ErrShelveExpiryRequired
	}
	c.mu.Lock()
	var activated []string
	until, err := c.shelveLocked(id, d, by, &activated)
	hook := c.onActivate
	evHook, events := c.drainEventsLocked()
	c.mu.Unlock()
	fireActivations(hook, activated)
	emitEvents(evHook, events)
	return until, err
}

func (c *Collector) shelveLocked(id string, d time.Duration, by string, activated *[]string) (time.Time, error) {
	e, ok := c.entries[id]
	if !ok || !c.visibleLocked(id, e, activated) {
		return time.Time{}, ErrUnknownAlarm
	}
	if !e.shelveable {
		return time.Time{}, fmt.Errorf("%w: %s — %s", ErrNotShelveable, e.alarm.TypeID,
			shelveRefusalReason(e.alarm))
	}
	if e.cleared {
		return time.Time{}, ErrAlarmCleared
	}
	now := c.now()
	until := now.Add(d)
	e.shelvedUntil = until
	e.acked, e.ackedBy, e.ackedAt = false, "", time.Time{}
	slog.Info("alarm shelved", "id", id, "shelved_by", by, "until", until)
	c.eventLocked(Event{
		Type:    EventAlarmShelved,
		Source:  e.alarm.Source,
		AlarmID: id,
		By:      by,
		Detail:  "shelved until " + until.UTC().Format(time.RFC3339),
	})
	c.journalAppendLocked(journalRecord{Op: journalOpShelve, ID: id, By: by, At: now, Until: until})
	return until, nil
}

// shelveRefusalReason is the operator-facing reason a type refuses shelve,
// surfaced by the API so the UI and CLI can show it.
func shelveRefusalReason(a Alarm) string {
	if a.SoftwareFault {
		return "a software fault cannot be deferred: nothing improves during the window; report it instead"
	}
	return "deferral is meaningless for this alarm type"
}

// Unshelve ends a shelve early, returning the alarm to its live state
// (active-unacked when the condition still stands). Returns ErrNotShelved
// when the alarm exists but is not shelved.
func (c *Collector) Unshelve(id string) error {
	c.mu.Lock()
	var activated []string
	err := c.unshelveLocked(id, &activated)
	hook := c.onActivate
	evHook, events := c.drainEventsLocked()
	c.mu.Unlock()
	fireActivations(hook, activated)
	emitEvents(evHook, events)
	return err
}

func (c *Collector) unshelveLocked(id string, activated *[]string) error {
	e, ok := c.entries[id]
	if !ok || !c.visibleLocked(id, e, activated) {
		return ErrUnknownAlarm
	}
	now := c.now()
	if !e.shelved(now) {
		return ErrNotShelved
	}
	e.shelvedUntil = time.Time{}
	slog.Info("alarm unshelved", "id", id)
	c.eventLocked(Event{
		Type:    EventAlarmUnshelved,
		Source:  e.alarm.Source,
		AlarmID: id,
		Detail:  "shelve ended early — returned to the active list",
	})
	c.journalAppendLocked(journalRecord{Op: journalOpUnshelve, ID: id, At: now})
	return nil
}

// journalAppendLocked persists a lifecycle record when a journal is
// attached. Caller holds c.mu.
func (c *Collector) journalAppendLocked(rec journalRecord) {
	if c.journal != nil {
		c.journal.append(rec)
	}
}
