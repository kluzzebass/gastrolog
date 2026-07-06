// Package locktrack provides a drop-in sync.RWMutex replacement that
// records who holds the lock and who is stuck waiting for it
// (gastrolog-1ug3rq). Born from a node-wide deadlock where an
// orchestrator read lock was acquired and never released: the leaker's
// goroutine had moved on, so no goroutine dump could name the
// acquisition site. With tracking on, the acquisition stack of every
// live hold is retained and a leak report names the exact line.
//
// Semantics assumed (true for the orchestrator, enforced by review):
// RUnlock is called on the same goroutine that called RLock. Cross-
// goroutine read-lock handoff would break holder matching; such an
// RUnlock is counted as unmatched rather than panicking.
//
// Cost when enabled: one short runtime.Stack call (goroutine ID) plus
// runtime.Callers per acquisition — a few microseconds. Orchestrator
// lock traffic is per-batch/per-RPC, not per-record, so this is noise
// at soak rates; GLOG_LOCK_TRACKING=off exists for when it is not.
// Cost when disabled: one atomic load per operation.
package locktrack

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// HoldKind distinguishes what a tracked entry represents.
type HoldKind string

const (
	HoldRead      HoldKind = "read-hold"
	HoldWrite     HoldKind = "write-hold"
	WaitWrite     HoldKind = "write-wait"
	stackDepthMax          = 24
)

// RWMutex wraps sync.RWMutex with optional holder/waiter tracking.
// The zero value is usable and starts with tracking disabled.
type RWMutex struct {
	mu sync.RWMutex

	tracking atomic.Bool

	state   sync.Mutex // guards everything below; critical sections are tiny
	nextID  uint64
	entries map[uint64]*entry
}

type entry struct {
	id       uint64
	gid      uint64
	kind     HoldKind
	since    time.Time
	pcs      []uintptr
	reported bool
}

// Leak describes a hold or wait that exceeded the report threshold.
type Leak struct {
	Kind  HoldKind
	Age   time.Duration
	Stack string
}

// EnableTracking turns holder/waiter recording on. Call at boot; safe
// to call at any time, but holds acquired while disabled are invisible.
func (m *RWMutex) EnableTracking() { m.tracking.Store(true) }

// TrackingEnabled reports whether recording is active.
func (m *RWMutex) TrackingEnabled() bool { return m.tracking.Load() }

func (m *RWMutex) Lock() {
	if !m.tracking.Load() {
		m.mu.Lock()
		return
	}
	gid := goroutineID()
	waitID := m.record(gid, WaitWrite)
	m.mu.Lock()
	m.state.Lock()
	if e := m.entries[waitID]; e != nil {
		// Promote the wait entry to a hold in place: same stack, new clock.
		e.kind = HoldWrite
		e.since = time.Now()
		e.reported = false
	}
	m.state.Unlock()
}

func (m *RWMutex) Unlock() {
	if m.tracking.Load() {
		m.releaseOne(goroutineID(), HoldWrite)
	}
	m.mu.Unlock()
}

func (m *RWMutex) TryLock() bool {
	ok := m.mu.TryLock()
	if ok && m.tracking.Load() {
		m.record(goroutineID(), HoldWrite)
	}
	return ok
}

func (m *RWMutex) RLock() {
	if !m.tracking.Load() {
		m.mu.RLock()
		return
	}
	m.mu.RLock()
	m.record(goroutineID(), HoldRead)
}

func (m *RWMutex) RUnlock() {
	if m.tracking.Load() {
		m.releaseOne(goroutineID(), HoldRead)
	}
	m.mu.RUnlock()
}

func (m *RWMutex) TryRLock() bool {
	ok := m.mu.TryRLock()
	if ok && m.tracking.Load() {
		m.record(goroutineID(), HoldRead)
	}
	return ok
}

func (m *RWMutex) record(gid uint64, kind HoldKind) uint64 {
	var pcs [stackDepthMax]uintptr
	// Skip runtime.Callers, record, and the RWMutex method — the caller's
	// frame is what names the acquisition site.
	n := runtime.Callers(3, pcs[:])
	m.state.Lock()
	defer m.state.Unlock()
	if m.entries == nil {
		m.entries = make(map[uint64]*entry)
	}
	m.nextID++
	id := m.nextID
	m.entries[id] = &entry{id: id, gid: gid, kind: kind, since: time.Now(), pcs: pcs[:n]}
	return id
}

// releaseOne removes this goroutine's most recent entry of the given
// kind. Unmatched releases (handoff, or acquired while tracking was
// off) are ignored.
func (m *RWMutex) releaseOne(gid uint64, kind HoldKind) {
	m.state.Lock()
	defer m.state.Unlock()
	var newest *entry
	for _, e := range m.entries {
		if e.gid != gid || e.kind != kind {
			continue
		}
		if newest == nil || e.id > newest.id {
			newest = e
		}
	}
	if newest != nil {
		delete(m.entries, newest.id)
	}
}

// Leaks returns entries older than threshold that have not been
// reported before, marking them reported. Waits and holds both count:
// a write waiter stuck for minutes is the symptom, a stale hold is the
// disease — the hold's stack is the payload that names the bug.
func (m *RWMutex) Leaks(threshold time.Duration) []Leak {
	now := time.Now()
	m.state.Lock()
	defer m.state.Unlock()
	var out []Leak
	for _, e := range m.entries {
		age := now.Sub(e.since)
		if age < threshold || e.reported {
			continue
		}
		e.reported = true
		out = append(out, Leak{Kind: e.kind, Age: age, Stack: formatStack(e.pcs)})
	}
	return out
}

// LiveCount returns the number of tracked entries (holds + waits).
func (m *RWMutex) LiveCount() int {
	m.state.Lock()
	defer m.state.Unlock()
	return len(m.entries)
}

func formatStack(pcs []uintptr) string {
	frames := runtime.CallersFrames(pcs)
	var b bytes.Buffer
	for {
		f, more := frames.Next()
		if f.Function != "" {
			fmt.Fprintf(&b, "%s\n\t%s:%d\n", f.Function, f.File, f.Line)
		}
		if !more {
			break
		}
	}
	return b.String()
}

// goroutineID parses the current goroutine's ID from its stack header
// ("goroutine 123 ["). ~1µs; only runs when tracking is enabled.
func goroutineID() uint64 {
	var buf [40]byte
	n := runtime.Stack(buf[:], false)
	s := buf[:n]
	s = bytes.TrimPrefix(s, []byte("goroutine "))
	if i := bytes.IndexByte(s, ' '); i > 0 {
		if id, err := strconv.ParseUint(string(s[:i]), 10, 64); err == nil {
			return id
		}
	}
	return 0
}
