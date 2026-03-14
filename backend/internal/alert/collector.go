// Package alert provides a thread-safe registry of runtime system alerts.
//
// Components push alerts via Set() when they detect degraded conditions,
// and clear them via Clear() when the condition resolves. The collector
// is a dumb registry — it stores what it's told. Each component owns
// the lifecycle of its own alerts.
package alert

import (
	"slices"
	"sync"
	"time"
)

// Severity indicates the urgency of an alert.
type Severity int

const (
	Warning Severity = 1
	Error   Severity = 2
)

// Alert represents a runtime condition that operators should be aware of.
type Alert struct {
	ID        string
	Severity  Severity
	Source    string   // component name (e.g. "orchestrator", "forwarder")
	Message   string
	FirstSeen time.Time
	LastSeen  time.Time
}

// Collector is a thread-safe, in-process registry of active alerts.
// Components call Set to raise alerts and Clear to resolve them.
type Collector struct {
	mu     sync.RWMutex
	alerts map[string]*Alert
}

// New creates a new alert collector.
func New() *Collector {
	return &Collector{
		alerts: make(map[string]*Alert),
	}
}

// Set raises or updates an alert. If an alert with this ID already exists,
// only LastSeen is updated (preserving FirstSeen and the original message).
// If it's new, both FirstSeen and LastSeen are set to now.
func (c *Collector) Set(id string, severity Severity, source, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if existing, ok := c.alerts[id]; ok {
		existing.LastSeen = now
		existing.Severity = severity
		existing.Message = message
		return
	}
	c.alerts[id] = &Alert{
		ID:        id,
		Severity:  severity,
		Source:    source,
		Message:   message,
		FirstSeen: now,
		LastSeen:  now,
	}
}

// Clear removes an alert. No-op if the alert doesn't exist.
func (c *Collector) Clear(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.alerts, id)
}

// Active returns a snapshot of all current alerts, sorted by FirstSeen.
func (c *Collector) Active() []*Alert {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.alerts) == 0 {
		return nil
	}
	result := make([]*Alert, 0, len(c.alerts))
	for _, a := range c.alerts {
		cp := *a
		result = append(result, &cp)
	}
	slices.SortFunc(result, func(a, b *Alert) int {
		return a.FirstSeen.Compare(b.FirstSeen)
	})
	return result
}

// AlertInfo is the struct returned by ActiveAlerts, matching the
// cluster.AlertInfo type without importing the cluster package.
type AlertInfo struct {
	ID        string
	Severity  int
	Source    string
	Message   string
	FirstSeen time.Time
	LastSeen  time.Time
}

// ActiveAlerts returns alerts in the format expected by the stats collector.
func (c *Collector) ActiveAlerts() []AlertInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.alerts) == 0 {
		return nil
	}
	result := make([]AlertInfo, 0, len(c.alerts))
	for _, a := range c.alerts {
		result = append(result, AlertInfo{
			ID:        a.ID,
			Severity:  int(a.Severity),
			Source:    a.Source,
			Message:   a.Message,
			FirstSeen: a.FirstSeen,
			LastSeen:  a.LastSeen,
		})
	}
	slices.SortFunc(result, func(a, b AlertInfo) int {
		return a.FirstSeen.Compare(b.FirstSeen)
	})
	return result
}

// Count returns the number of active alerts.
func (c *Collector) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.alerts)
}
