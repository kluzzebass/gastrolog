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
//     operator acknowledgment. INTERIM: acknowledgment does not exist until
//     the lifecycle phase (gastrolog-1z5gg4), so a latched alarm remains
//     standing with no way to clear it — exactly the sticky behavior the
//     latched types had by convention before; the ack phase makes them
//     clearable.
//
// Suppression windows are evaluated LAZILY: state advances on every
// Raise/Clear touching an alarm and on every read (Active/Count), against
// the collector's injectable clock. A condition raised once and never
// re-raised still activates once its DelayOn elapses — the next read
// surfaces it. FirstSeen is the CONDITION start (the first Raise of the
// occurrence), not the moment the alarm activated: the delay-on window
// suppresses annunciation, not the condition's history.
//
// Operator-defined alarms — whose priority comes from an operator-configured
// rule rather than the catalog (e.g. vault rate thresholds) — enter through
// RaiseOperator() and live beside the catalog, never inside it. They carry
// no suppression: the rule is the condition definition.
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

// Alarm is one active alarm instance: a cataloged (or operator-defined)
// type plus the instance it fired for. ID is the stable dedup key,
// composed from the type ID and instance key.
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

// OperatorAlarm is an alarm whose priority and guidance come from an
// operator-configured rule rather than the static catalog. It is modeled
// beside the catalog: RaiseOperator stores it verbatim.
type OperatorAlarm struct {
	TypeID      string
	InstanceKey string
	Priority    Priority
	Source      string
	Detail      string
	Cause       string
	Response    string
}

// Sink is the raising side of the collector, satisfied by *Collector. It is
// THE alarm-sink interface — components that raise alarms depend on this one
// type rather than declaring structurally identical local copies.
type Sink interface {
	Raise(typeID, instanceKey, detail string)
	RaiseOperator(a OperatorAlarm)
	Clear(typeID, instanceKey string)
}

// entry is the collector's per-alarm suppression state machine. The alarm
// inside it is only visible through Active() once the entry has activated.
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
	}, t.DelayOn, t.DelayOff, t.Latching)
}

// RaiseOperator raises or refreshes an operator-defined alarm. The rule that
// defined the threshold supplies priority, cause and response; there is no
// catalog entry and no suppression.
func (c *Collector) RaiseOperator(a OperatorAlarm) {
	c.raise(Alarm{
		ID:          alarmID(a.TypeID, a.InstanceKey),
		TypeID:      a.TypeID,
		InstanceKey: a.InstanceKey,
		Priority:    a.Priority,
		Source:      a.Source,
		Detail:      a.Detail,
		Cause:       a.Cause,
		Response:    a.Response,
	}, 0, 0, false)
}

// raise records that the condition for a is up, creating or refreshing its
// suppression entry. Detail and priority refresh on every raise (operator-
// defined alarms may escalate; cataloged priorities are static but
// harmlessly re-stamped); FirstSeen is preserved across the occurrence.
func (c *Collector) raise(a Alarm, delayOn, delayOff time.Duration, latching bool) {
	c.mu.Lock()
	var activated []string

	now := c.now()
	e := c.entries[a.ID]
	if e != nil && c.settleLocked(e, now, &activated) {
		// The previous occurrence's delay-off window had already expired;
		// this raise starts a fresh one.
		delete(c.entries, a.ID)
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
		c.settleLocked(e, now, &activated) // zero DelayOn activates immediately
	} else {
		firstSeen := e.alarm.FirstSeen
		e.alarm = a
		e.alarm.FirstSeen = firstSeen
		e.alarm.LastSeen = now
		e.delayOn, e.delayOff, e.latching = delayOn, delayOff, latching
		if !e.conditionUp {
			// The condition returned inside the delay-off window (or on a
			// latched alarm): the same occurrence continues — the alarm stays
			// active with its FirstSeen, no re-occurrence, no activation edge.
			e.conditionUp = true
			e.clearedAt = time.Time{}
		}
		c.settleLocked(e, now, &activated)
	}
	hook := c.onActivate
	c.mu.Unlock()
	fireActivations(hook, activated)
}

// Clear reports that the condition of the given type for the given instance
// no longer holds. No-op if nothing is tracked for it. What happens next is
// the type's suppression verdict:
//
//   - never activated (condition died inside DelayOn): dropped silently —
//     that is the chattering the window exists to suppress.
//   - active, Latching: stays active. INTERIM until the lifecycle phase
//     (gastrolog-1z5gg4) ships acknowledgment, a latched alarm has no way
//     to clear — today's sticky behavior, unchanged; ack will clear it.
//   - active, DelayOff zero: clears immediately (the pre-suppression
//     behavior of every type).
//   - active, DelayOff set: stays active until the condition has stayed
//     clear for the whole window; a Raise inside it resumes the same
//     occurrence.
func (c *Collector) Clear(typeID, instanceKey string) {
	c.mu.Lock()
	var activated []string
	c.clearLocked(typeID, instanceKey, &activated)
	hook := c.onActivate
	c.mu.Unlock()
	// A pending condition whose delay-on elapsed before this Clear arrived
	// annunciated during settling — it outlived its window, so it counts.
	fireActivations(hook, activated)
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
		delete(c.entries, id)
		return
	}
	if !e.active {
		// The condition never outlived its delay-on window: suppressed.
		delete(c.entries, id)
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
		slog.Info("alarm condition cleared but the alarm is latched — standing until operator acknowledgment",
			"id", e.alarm.ID, "source", e.alarm.Source)
		return
	}
	if e.delayOff <= 0 {
		if e.delayOn > 0 {
			// This alarm's annunciation was logged by the collector
			// (activation edge below); log the matching resolution edge.
			slog.Info("alarm cleared — condition resolved",
				"id", e.alarm.ID, "source", e.alarm.Source)
		}
		delete(c.entries, id)
	}
}

// settleLocked advances one entry's suppression state to now: it activates
// a pending entry whose condition has outlived DelayOn, and reports (true)
// an active entry whose condition has stayed clear past DelayOff so the
// caller removes it. Transition edges of suppressed types are logged here —
// the call site cannot log them, since it no longer knows when the window
// elapses. Annunciations are appended to activated for the caller to fire
// through the activation hook after unlocking. Caller holds c.mu.
func (c *Collector) settleLocked(e *entry, now time.Time, activated *[]string) (expired bool) {
	if e.conditionUp {
		if !e.active && now.Sub(e.conditionSince) >= e.delayOn {
			e.active = true
			*activated = append(*activated, e.alarm.TypeID)
			if e.delayOn > 0 {
				slog.Warn("alarm active — condition persisted past its delay-on window",
					"id", e.alarm.ID, "source", e.alarm.Source, "delay_on", e.delayOn, "detail", e.alarm.Detail)
			}
		}
		return false
	}
	if e.active && !e.latching && now.Sub(e.clearedAt) >= e.delayOff {
		if e.delayOff > 0 {
			slog.Info("alarm cleared — condition stayed clear past its delay-off window",
				"id", e.alarm.ID, "source", e.alarm.Source, "delay_off", e.delayOff)
		}
		return true
	}
	return false
}

// Active returns a snapshot of all currently active alarms, sorted by
// FirstSeen. Reading settles suppression state: pending conditions whose
// delay-on window has elapsed activate here even if never re-raised, and
// cleared conditions past their delay-off window drop out.
func (c *Collector) Active() []*Alarm {
	c.mu.Lock()
	var activated []string

	now := c.now()
	var result []*Alarm
	for id, e := range c.entries {
		if c.settleLocked(e, now, &activated) {
			delete(c.entries, id)
			continue
		}
		if !e.active {
			continue
		}
		cp := e.alarm
		result = append(result, &cp)
	}
	hook := c.onActivate
	c.mu.Unlock()
	// Delayed conditions may annunciate on a read — the rate monitor must
	// still see them.
	fireActivations(hook, activated)

	if len(result) == 0 {
		return nil
	}
	slices.SortFunc(result, func(a, b *Alarm) int {
		return a.FirstSeen.Compare(b.FirstSeen)
	})
	return result
}

// Count returns the number of active alarms (pending delay-on conditions
// excluded), settling suppression state like Active.
func (c *Collector) Count() int {
	c.mu.Lock()
	var activated []string

	now := c.now()
	n := 0
	for id, e := range c.entries {
		if c.settleLocked(e, now, &activated) {
			delete(c.entries, id)
			continue
		}
		if e.active {
			n++
		}
	}
	hook := c.onActivate
	c.mu.Unlock()
	fireActivations(hook, activated)
	return n
}
