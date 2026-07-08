package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"gastrolog/internal/alert"
	"gastrolog/internal/units"
)

// Disk-space guard (independently shippable slice of the pipeline
// backpressure epic). The 2026-07-08 incident: unbounded pipeline backlog
// filled the volume over ~4.5h with zero operator signal; the first symptom
// was three simultaneous node panics on Raft WAL fsync ENOSPC — the worst
// possible outcome, because it kills consensus first.
//
// Two thresholds, both on the minimum free space across the node's data
// paths:
//
//   - WARN: raise the disk-space alarm (operator action: free space, add
//     capacity, or shorten retention). Cleared with hysteresis.
//   - FLOOR: additionally enter protect mode — the node stops ACCEPTING new
//     work (ingest admission rejects, catch-up pulls pause, pressure-aware
//     ingesters see Critical) while consensus and the paths that FREE space
//     (retention expunge, delete acks) keep running.
const (
	diskGuardJobName  = "disk-guard"
	diskGuardSchedule = "*/15 * * * * *" // every 15 seconds

	diskGuardAlertID = "disk-space"

	// Fractions of the volume, paired with absolute byte minimums. The
	// larger of the two governs — but the absolute minimums are CLAMPED
	// to a share of the volume so a small (or quota-capped) volume isn't
	// permanently in alarm: a 10GB test volume must not carry a 10GiB
	// warn threshold.
	diskFreeWarnFraction  = 0.10
	diskFreeFloorFraction = 0.03
	diskFreeWarnBytes     = uint64(10 << 30) // 10 GiB
	diskFreeFloorBytes    = uint64(3 << 30)  // 3 GiB
	diskFreeWarnMaxShare  = 0.25             // warn threshold ≤ 25% of the volume
	diskFreeFloorMaxShare = 0.10             // floor threshold ≤ 10% of the volume

	// Hysteresis multipliers so the alarm and protect mode don't chatter
	// at the boundary (EEMUA: deadbands, not flapping).
	diskGuardClearFactor = 1.25
)

// ErrDiskProtect rejects new work while the node is below its free-space
// floor. Producers treat it as retryable backpressure.
var ErrDiskProtect = errors.New("node is out of disk space: ingest admission suspended until space is freed")

// diskGuard samples free space on the node's data paths and drives the
// alarm + protect state. sample is injectable for tests.
type diskGuard struct {
	paths  []string
	sample func(path string) (free, total uint64, err error)

	warnFraction  float64
	floorFraction float64
	warnBytes     uint64
	floorBytes    uint64

	protect atomic.Bool

	mu          sync.Mutex
	alarmRaised bool
}

func newDiskGuard(paths []string) *diskGuard {
	g := &diskGuard{
		paths:         paths,
		sample:        statfsSample,
		warnFraction:  diskFreeWarnFraction,
		floorFraction: diskFreeFloorFraction,
		warnBytes:     diskFreeWarnBytes,
		floorBytes:    diskFreeFloorBytes,
	}
	// Operator overrides in whole GiB via the .env channel.
	if v, err := strconv.ParseUint(os.Getenv("GLOG_DISK_FREE_WARN_GB"), 10, 32); err == nil && v > 0 {
		g.warnBytes = v << 30
	}
	if v, err := strconv.ParseUint(os.Getenv("GLOG_DISK_FREE_FLOOR_GB"), 10, 32); err == nil && v > 0 {
		g.floorBytes = v << 30
	}
	return g
}

func statfsSample(path string) (uint64, uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bsize := uint64(st.Bsize)
	return st.Bavail * bsize, st.Blocks * bsize, nil
}

// worstFree returns the minimum free bytes and its volume total across the
// guarded paths (deduplicating exact repeats is unnecessary — sampling the
// same volume twice yields the same numbers).
func (g *diskGuard) worstFree() (free, total uint64, ok bool) {
	first := true
	for _, p := range g.paths {
		f, t, err := g.sample(p)
		if err != nil || t == 0 {
			continue
		}
		if first || f < free {
			free, total = f, t
			first = false
		}
	}
	return free, total, !first
}

func (g *diskGuard) warnThreshold(total uint64) uint64 {
	t := max(uint64(float64(total)*g.warnFraction), g.warnBytes)
	return min(t, uint64(float64(total)*diskFreeWarnMaxShare))
}

func (g *diskGuard) floorThreshold(total uint64) uint64 {
	t := max(uint64(float64(total)*g.floorFraction), g.floorBytes)
	return min(t, uint64(float64(total)*diskFreeFloorMaxShare))
}

// evaluate runs one guard pass: updates protect mode and raises/clears the
// disk-space alarm on the given collector. Scheduler-driven.
func (g *diskGuard) evaluate(alerts AlertCollector) {
	free, total, ok := g.worstFree()
	if !ok {
		return // no sampleable path; nothing trustworthy to act on
	}
	warnAt := g.warnThreshold(total)
	floorAt := g.floorThreshold(total)

	// Protect mode: enter at the floor, exit with hysteresis.
	if g.protect.Load() {
		if free > uint64(float64(floorAt)*diskGuardClearFactor) {
			g.protect.Store(false)
		}
	} else if free < floorAt {
		g.protect.Store(true)
	}

	if alerts == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case free < warnAt:
		msg := fmt.Sprintf(
			"Low disk space: %s free of %s. Free space, add capacity, or shorten retention.",
			units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total))) //nolint:gosec // display only
		if g.protect.Load() {
			msg = fmt.Sprintf(
				"Out of disk space: %s free of %s — ingest admission is SUSPENDED on this node until space is freed. Retention and deletes keep running.",
				units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total))) //nolint:gosec // display only
		}
		alerts.Set(diskGuardAlertID, alert.Error, "storage", msg)
		g.alarmRaised = true
	case g.alarmRaised && free > uint64(float64(warnAt)*diskGuardClearFactor):
		alerts.Clear(diskGuardAlertID)
		g.alarmRaised = false
	}
}

// diskProtectActive reports whether the node is refusing new work for lack
// of disk space. Consulted by ingest admission and the catch-up puller.
func (o *Orchestrator) diskProtectActive() bool {
	return o.diskGuard != nil && o.diskGuard.protect.Load()
}

// diskAdmissionGate is the supervisor's admission check: reject new records
// while the node is below its free-space floor.
func (o *Orchestrator) diskAdmissionGate() error {
	if o.diskProtectActive() {
		return ErrDiskProtect
	}
	return nil
}

// startDiskGuard registers the guard's scheduler job. No-op without paths.
func (o *Orchestrator) startDiskGuard() error {
	if o.diskGuard == nil || len(o.diskGuard.paths) == 0 {
		return nil
	}
	if err := o.scheduler.AddJob(diskGuardJobName, diskGuardSchedule, func() {
		o.diskGuard.evaluate(o.alerts)
	}); err != nil {
		return fmt.Errorf("disk guard: %w", err)
	}
	o.scheduler.Describe(diskGuardJobName,
		"Free-space guard: raises the disk-space alarm and suspends ingest admission below the floor")
	return nil
}
