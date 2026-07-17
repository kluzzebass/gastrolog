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
// Operator-defined alarms — whose priority comes from an operator-configured
// rule rather than the catalog (e.g. vault rate thresholds) — enter through
// RaiseOperator() and live beside the catalog, never inside it.
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

// Collector is a thread-safe, in-process registry of active alarms.
type Collector struct {
	mu     sync.RWMutex
	alarms map[string]*Alarm
	// onActivate, when set, is invoked after an alarm transitions
	// inactive → active (a new ID entering the registry) — never on a
	// refresh of an already-active alarm. Called outside the collector
	// lock. Wired to the rate monitor (RateMonitor.Observe).
	onActivate func(typeID string)
}

// SetOnActivate installs the activation hook. Wire once at startup, before
// components start raising.
func (c *Collector) SetOnActivate(fn func(typeID string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onActivate = fn
}

// New creates a new alarm collector.
func New() *Collector {
	return &Collector{
		alarms: make(map[string]*Alarm),
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

// Raise raises or refreshes the alarm of the given cataloged type for the
// given instance. The collector stamps priority, source, cause and response
// from the catalog; detail carries the per-instance specifics.
//
// Raising an unregistered type is a catalog defect in the raising component.
// It is loud, never silent: the defect is logged AND the condition still
// surfaces — as a software-fault alarm with the raiser's detail, so neither
// the underlying condition nor the defect can disappear.
func (c *Collector) Raise(typeID, instanceKey, detail string) {
	t, ok := TypeByID(typeID)
	if !ok {
		slog.Error("alarm raised for a type missing from the alarm catalog — software defect in the raising component",
			"type", typeID, "instance", instanceKey, "detail", detail)
		t = unregisteredAlarmType(typeID)
	}
	c.upsert(Alarm{
		ID:            alarmID(typeID, instanceKey),
		TypeID:        typeID,
		InstanceKey:   instanceKey,
		Priority:      t.Priority,
		SoftwareFault: t.SoftwareFault,
		Source:        t.Source,
		Detail:        detail,
		Cause:         t.Cause,
		Response:      t.Response,
	})
}

// RaiseOperator raises or refreshes an operator-defined alarm. The rule that
// defined the threshold supplies priority, cause and response.
func (c *Collector) RaiseOperator(a OperatorAlarm) {
	c.upsert(Alarm{
		ID:          alarmID(a.TypeID, a.InstanceKey),
		TypeID:      a.TypeID,
		InstanceKey: a.InstanceKey,
		Priority:    a.Priority,
		Source:      a.Source,
		Detail:      a.Detail,
		Cause:       a.Cause,
		Response:    a.Response,
	})
}

// upsert stores an alarm, preserving FirstSeen for an already-active ID.
// Detail and priority refresh on every raise (operator-defined alarms may
// escalate; cataloged priorities are static but harmlessly re-stamped).
func (c *Collector) upsert(a Alarm) {
	c.mu.Lock()
	now := time.Now()
	existing, refresh := c.alarms[a.ID]
	if refresh {
		a.FirstSeen = existing.FirstSeen
	} else {
		a.FirstSeen = now
	}
	a.LastSeen = now
	c.alarms[a.ID] = &a
	onActivate := c.onActivate
	c.mu.Unlock()

	// Activation hook outside the lock: the hook may raise back into the
	// collector (the rate monitor raising alarm-flood).
	if !refresh && onActivate != nil {
		onActivate(a.TypeID)
	}
}

// Clear resolves the alarm of the given type for the given instance. No-op
// if it is not active.
func (c *Collector) Clear(typeID, instanceKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.alarms, alarmID(typeID, instanceKey))
}

// Active returns a snapshot of all current alarms, sorted by FirstSeen.
func (c *Collector) Active() []*Alarm {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.alarms) == 0 {
		return nil
	}
	result := make([]*Alarm, 0, len(c.alarms))
	for _, a := range c.alarms {
		cp := *a
		result = append(result, &cp)
	}
	slices.SortFunc(result, func(a, b *Alarm) int {
		return a.FirstSeen.Compare(b.FirstSeen)
	})
	return result
}

// Count returns the number of active alarms.
func (c *Collector) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.alarms)
}
