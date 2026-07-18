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
//     becomes standing. A condition that flaps below the window never
//     annunciates.
//   - DelayOff: after a standing alarm's condition clears, it must STAY
//     clear that long before the alarm auto-clears. A re-raise inside the
//     window is the same continuous occurrence, not a new one.
//   - Latching: plain sticky — the alarm stays standing after the condition
//     clears, until process restart. There is no release path, and that is
//     intentional: the one latching type is a software-fault tripwire, and
//     the response to a software fault is report + restart.
//
// Alarms are STATE with suppression, nothing more: an alarm is standing
// while its condition holds and releases when the condition resolves.
// There are no per-alarm operator states — an alarm is standing or it is
// not. Acknowledgment (extra awareness states plus an on-disk journal),
// operator-bounded suppression and rate self-monitoring (a flood
// meta-alarm) were all built here and removed on operator verdict: the
// epic's purpose was alarm REDUCTION, and management
// machinery presumes a rich alarm ecosystem worth managing — the opposite
// of the goal. Nothing persists across restart — after a restart a
// re-detected condition is simply a standing alarm again. See the
// state-model section of docs/alarm-management-design.md for the recorded
// decisions.
//
// Suppression windows are evaluated LAZILY: state advances on every
// Raise/Clear touching an alarm and on every read (Standing/Count),
// against the collector's injectable clock. A condition raised once and
// never re-raised still annunciates once its DelayOn elapses — the next
// read surfaces it. FirstSeen is the CONDITION start (the first Raise of
// the occurrence), not the moment the alarm annunciated: the delay-on
// window suppresses annunciation, not the condition's history.
//
// There is no path around the catalog: every alarm enters through Raise,
// and priority is never chosen at a call site.
package alert

import (
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
}

// Sink is the raising side of the collector, satisfied by *Collector. It is
// THE alarm-sink interface — components that raise alarms depend on this one
// type rather than declaring structurally identical local copies.
type Sink interface {
	Raise(typeID, instanceKey, detail string)
	Clear(typeID, instanceKey string)
}

// entry is the collector's per-alarm suppression state machine. The alarm
// inside it is only visible through Standing() once the entry has
// annunciated.
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
	// standing — the DelayOff window measures from here.
	clearedAt time.Time
	// active is whether the alarm has annunciated: the condition outlived
	// DelayOn (immediately, for zero-delay types).
	active bool
}

// Collector is a thread-safe, in-process registry of standing alarms with
// catalog-driven chattering suppression.
type Collector struct {
	mu      sync.Mutex
	entries map[string]*entry

	// now is the collector's clock. Injectable (NewWithClock) so
	// suppression tests advance time deterministically — never with
	// sleeps.
	now func() time.Time
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
// DelayOn the alarm annunciates only once the condition has persisted that
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
	}, t.DelayOn, t.DelayOff, t.Latching)
}

// raise records that the condition for a is up, creating or refreshing its
// suppression entry. Detail refreshes on every raise (the cataloged fields
// are static but harmlessly re-stamped); FirstSeen is preserved across the
// occurrence.
func (c *Collector) raise(a Alarm, delayOn, delayOff time.Duration, latching bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	e := c.entries[a.ID]
	if e != nil && c.settleLocked(e, now) {
		// The previous occurrence had fully released (its delay-off window
		// expired); this raise starts a fresh entry.
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
		}
		a.FirstSeen = now
		a.LastSeen = now
		e.alarm = a
		c.entries[a.ID] = e
		c.settleLocked(e, now) // zero DelayOn annunciates immediately
		return
	}
	firstSeen := e.alarm.FirstSeen
	e.alarm = a
	e.alarm.FirstSeen = firstSeen
	e.alarm.LastSeen = now
	e.delayOn, e.delayOff, e.latching = delayOn, delayOff, latching
	if !e.conditionUp {
		// The condition returned inside the delay-off window (or on a
		// latched alarm): the same occurrence continues — the alarm stays
		// standing with its FirstSeen, no re-occurrence.
		e.conditionUp = true
		e.clearedAt = time.Time{}
	}
	c.settleLocked(e, now)
}

// Clear reports that the condition of the given type for the given instance
// no longer holds. No-op if nothing is tracked for it. What happens next is
// the type's suppression verdict:
//
//   - never annunciated (condition died inside DelayOn): dropped silently —
//     that is the chattering the window exists to suppress.
//   - standing, Latching: stays standing until process restart — there is no
//     release path for a latched alarm.
//   - standing, non-latching: released once DelayOff has run (immediately
//     for zero). A Raise inside the window resumes the same occurrence.
func (c *Collector) Clear(typeID, instanceKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := alarmID(typeID, instanceKey)
	e, ok := c.entries[id]
	if !ok {
		return
	}
	now := c.now()
	if c.settleLocked(e, now) {
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
		slog.Info("alarm condition cleared but the alarm is latched — standing until process restart",
			"id", e.alarm.ID, "source", e.alarm.Source)
		return
	}
	if e.delayOff <= 0 {
		slog.Info("alarm cleared — condition resolved",
			"id", e.alarm.ID, "source", e.alarm.Source)
		c.removeLocked(id)
	}
}

// settleLocked advances one entry's suppression state to now: it annunciates
// a pending entry whose condition has outlived DelayOn, and releases a
// standing entry whose condition has stayed clear past DelayOff (report true
// so the caller removes it). Transition edges of suppressed types are logged
// here — the call site cannot log them, since it no longer knows when the
// window elapses. Caller holds c.mu.
func (c *Collector) settleLocked(e *entry, now time.Time) (expired bool) {
	if e.conditionUp {
		if !e.active && now.Sub(e.conditionSince) >= e.delayOn {
			c.activateLocked(e)
		}
		return false
	}
	if e.active && !e.latching && now.Sub(e.clearedAt) >= e.delayOff {
		// The condition resolved when its delay-off window closed: the
		// alarm releases, full stop — lazy settling reaches the same state
		// no matter when the next read runs.
		slog.Info("alarm cleared — condition stayed clear past its delay-off window",
			"id", e.alarm.ID, "source", e.alarm.Source, "delay_off", e.delayOff)
		return true
	}
	return false
}

// activateLocked annunciates an entry whose condition has outlived its
// delay-on window. Caller holds c.mu.
func (c *Collector) activateLocked(e *entry) {
	e.active = true
	// FirstSeen is the condition start of this occurrence.
	e.alarm.FirstSeen = e.conditionSince
	// Every annunciation edge logs exactly once — the log stream is the
	// event record (events are log messages; the self ingester captures
	// them). Call sites cannot log this edge: they no longer know when a
	// delay-on window elapses, and zero-delay raisers refresh every tick.
	if e.delayOn > 0 {
		slog.Warn("alarm active — condition persisted past its delay-on window",
			"id", e.alarm.ID, "source", e.alarm.Source, "delay_on", e.delayOn, "detail", e.alarm.Detail)
	} else {
		slog.Warn("alarm raised",
			"id", e.alarm.ID, "source", e.alarm.Source, "detail", e.alarm.Detail)
	}
}

// removeLocked releases an entry entirely. Caller holds c.mu.
func (c *Collector) removeLocked(id string) {
	delete(c.entries, id)
}

// Standing returns a snapshot of every standing alarm, sorted by FirstSeen.
// This is the one read surface — the broadcast (NodeStats), the CLI and the
// UI all render this list. Reading settles suppression state lazily:
// pending delay-on conditions annunciate here once their window elapses.
func (c *Collector) Standing() []*Alarm {
	c.mu.Lock()
	now := c.now()
	var result []*Alarm
	for id, e := range c.entries {
		if c.settleLocked(e, now) {
			c.removeLocked(id)
			continue
		}
		if !e.active {
			continue
		}
		cp := e.alarm
		result = append(result, &cp)
	}
	c.mu.Unlock()

	if len(result) == 0 {
		return nil
	}
	slices.SortFunc(result, func(a, b *Alarm) int {
		return a.FirstSeen.Compare(b.FirstSeen)
	})
	return result
}

// Count returns the number of standing alarms (the same set Standing
// returns), settling state like Standing.
func (c *Collector) Count() int {
	return len(c.Standing())
}
