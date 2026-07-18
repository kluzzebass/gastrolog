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
//   - Latching: plain sticky — the alarm stays standing after the condition
//     clears, until process restart. There is no release path, and that is
//     intentional: the one latching type is a software-fault tripwire, and
//     the response to a software fault is report + restart.
//
// Alarms are STATE with suppression, nothing more: an alarm is ACTIVE while
// its condition holds and releases when the condition resolves, and an
// operator can temporarily SHELVE it (bounded, deliberate suppression with
// a mandatory expiry). Nothing persists across restart — after a restart a
// re-detected condition is simply an active alarm again. An acknowledgment
// layer (extra operator-awareness states plus an on-disk journal) was
// built here and removed on operator verdict: awareness bookkeeping is
// ceremony, and loud is safe. See the state-model section of
// docs/alarm-management-design.md for the recorded decision.
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

// AlarmState is the state of a standing alarm, derived from the suppression
// substrate plus the operator's shelve action on every read. Mirrors the
// proto AlarmState enum.
type AlarmState int

const (
	// StateActive: annunciated and standing. Also a latched alarm whose
	// condition resolved — it stands until process restart.
	StateActive AlarmState = 1
	// StateShelved: operator-shelved until an expiry. Never permanent.
	StateShelved AlarmState = 2
)

// String returns the operator-facing label for an alarm state.
func (s AlarmState) String() string {
	switch s {
	case StateActive:
		return "active"
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

	// State is derived at snapshot time; ShelvedUntil is set only while
	// shelved.
	State        AlarmState
	ShelvedUntil time.Time
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
// shelve state on top of it. The alarm inside it is only visible through
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

	// shelvedUntil is the mandatory shelve expiry; zero when not shelved.
	// Expiry is settled lazily against the collector clock. In-memory
	// only: shelves do not survive restart.
	shelvedUntil time.Time
	// shelveable is the catalog verdict, stamped at raise time.
	shelveable bool
}

// shelved reports whether the entry is currently shelved at instant now.
func (e *entry) shelved(now time.Time) bool {
	return !e.shelvedUntil.IsZero() && now.Before(e.shelvedUntil)
}

// stateAt derives the state of an annunciated entry.
func (e *entry) stateAt(now time.Time) AlarmState {
	if e.shelved(now) {
		return StateShelved
	}
	// Includes a latched alarm whose condition resolved: it stands until
	// process restart.
	return StateActive
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
//   - active, Latching: stays standing until process restart — there is no
//     release path for a latched alarm.
//   - active, non-latching: released once DelayOff has run (immediately for
//     zero). A Raise inside the window resumes the same occurrence.
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

// settleLocked advances one entry's suppression state to now: it expires a
// lapsed shelve, activates a pending entry whose condition has outlived
// DelayOn, and releases an active entry whose condition has stayed clear
// past DelayOff (report true so the caller removes it). Transition edges of
// suppressed types are logged here — the call site cannot log them, since
// it no longer knows when the window elapses. Annunciations are appended to
// activated for the caller to fire through the activation hook after
// unlocking. Caller holds c.mu.
func (c *Collector) settleLocked(e *entry, now time.Time, activated *[]string) (expired bool) {
	// Shelve expiry is a lazy time construct like the delay windows. A
	// shelve that lapses returns the alarm to ACTIVE: the deferral window
	// the operator chose is over, so the condition demands fresh attention.
	if !e.shelvedUntil.IsZero() && !now.Before(e.shelvedUntil) {
		e.shelvedUntil = time.Time{}
		slog.Info("alarm shelve expired — returned to the active list",
			"id", e.alarm.ID, "source", e.alarm.Source)
	}
	if e.conditionUp {
		if !e.active && now.Sub(e.conditionSince) >= e.delayOn {
			c.activateLocked(e, activated)
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
func (c *Collector) activateLocked(e *entry, activated *[]string) {
	e.active = true
	// FirstSeen is the condition start of this occurrence.
	e.alarm.FirstSeen = e.conditionSince
	*activated = append(*activated, e.alarm.TypeID)
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

// snapshot walks all entries under the lock, settles their suppression
// state, and returns copies of the annunciated ones that pass keep. Sorted
// by FirstSeen.
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
		if st == StateShelved {
			cp.ShelvedUntil = e.shelvedUntil
		}
		cp.Shelveable = e.shelveable
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

// isActiveState reports whether a state demands a place in the active
// list: the condition is standing (or latched standing) and not
// operator-suppressed.
func isActiveState(s AlarmState) bool {
	return s == StateActive
}

// Standing returns a snapshot of every visible alarm — active and shelved —
// each stamped with its state, sorted by FirstSeen. This is the broadcast
// surface: alarms in every state travel in NodeStats so any node can serve
// shelve/unshelve for them. Reading settles suppression state lazily.
func (c *Collector) Standing() []*Alarm {
	return c.snapshot(func(AlarmState) bool { return true })
}

// Active returns a snapshot of the active alarms only — pending delay-on
// conditions and shelved alarms are excluded — sorted by FirstSeen.
func (c *Collector) Active() []*Alarm {
	return c.snapshot(isActiveState)
}

// Count returns the number of active alarms (the same set Active returns),
// settling state like Active.
func (c *Collector) Count() int {
	return len(c.Active())
}

// HasStanding reports whether an alarm with the given full ID is currently
// visible in any state on this collector.
func (c *Collector) HasStanding(id string) bool {
	for _, a := range c.Standing() {
		if a.ID == id {
			return true
		}
	}
	return false
}

// visibleLocked settles e and returns whether it is annunciated (visible in
// some state). Caller holds c.mu; removal on release is applied.
func (c *Collector) visibleLocked(id string, e *entry, activated *[]string) bool {
	if c.settleLocked(e, c.now(), activated) {
		c.removeLocked(id)
		return false
	}
	return e.active
}

// Shelve suppresses the standing alarm with the given full ID until now+d.
// The expiry is mandatory (d must be positive — there are no permanent
// shelves) and the alarm's type must allow shelving. Shelve state is
// in-memory only and does not survive restart. Returns the expiry instant.
func (c *Collector) Shelve(id string, d time.Duration, by string) (time.Time, error) {
	if d <= 0 {
		return time.Time{}, ErrShelveExpiryRequired
	}
	c.mu.Lock()
	var activated []string
	until, err := c.shelveLocked(id, d, by, &activated)
	hook := c.onActivate
	c.mu.Unlock()
	fireActivations(hook, activated)
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
	until := c.now().Add(d)
	e.shelvedUntil = until
	slog.Info("alarm shelved", "id", id, "shelved_by", by, "until", until)
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

// Unshelve ends a shelve early, returning the alarm to ACTIVE. Returns
// ErrNotShelved when the alarm exists but is not shelved.
func (c *Collector) Unshelve(id string) error {
	c.mu.Lock()
	var activated []string
	err := c.unshelveLocked(id, &activated)
	hook := c.onActivate
	c.mu.Unlock()
	fireActivations(hook, activated)
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
	return nil
}
