// Package sysmetrics tracks process-level CPU and memory usage.
package sysmetrics

import (
	"runtime"
	"runtime/metrics"
	"sync"
	"syscall"
	"time"
)

// cpuTracker tracks CPU usage between successive calls. It is safe for
// concurrent use. The clock and rusage fields allow dependency injection
// for testing.
type cpuTracker struct {
	mu       sync.Mutex
	lastWall time.Time
	lastUser time.Duration
	lastSys  time.Duration
	lastCPU  float64
	clock    func() time.Time
	rusage   func() (user, sys time.Duration)
}

func newCPUTracker(clock func() time.Time, rusage func() (user, sys time.Duration)) *cpuTracker {
	u, s := rusage()
	return &cpuTracker{
		lastWall: clock(),
		lastUser: u,
		lastSys:  s,
		clock:    clock,
		rusage:   rusage,
	}
}

// minWindow is the minimum wall-clock duration between baseline resets.
// Multiple callers within this window get the same stable reading instead
// of racing over progressively shorter (and noisier) measurement windows.
const minWindow = 2 * time.Second

func (t *cpuTracker) percent() float64 {
	now := t.clock()
	utime, stime := t.rusage()

	t.mu.Lock()
	defer t.mu.Unlock()

	wall := now.Sub(t.lastWall)
	if wall < minWindow {
		return t.lastCPU
	}

	cpuDelta := (utime - t.lastUser) + (stime - t.lastSys)
	pct := float64(cpuDelta) / float64(wall) * 100.0

	t.lastWall = now
	t.lastUser = utime
	t.lastSys = stime
	t.lastCPU = pct

	return pct
}

var defaultTracker = newCPUTracker(time.Now, getrusageTimes)

// CPUPercent returns the process CPU usage as a percentage (0–100+)
// since the last call. Multi-core processes can exceed 100%.
func CPUPercent() float64 { return defaultTracker.percent() }

// MemoryStats holds a detailed memory breakdown.
type MemoryStats struct {
	// Inuse is HeapInuse + StackInuse (summary value for the header).
	Inuse int64
	// RSS is the peak resident set size from the OS (getrusage Maxrss).
	RSS int64
	// HeapAlloc is bytes of live heap objects.
	HeapAlloc int64
	// HeapInuse is bytes in in-use heap spans.
	HeapInuse int64
	// HeapIdle is bytes in idle (unused) heap spans.
	HeapIdle int64
	// HeapReleased is heap bytes released back to the OS.
	HeapReleased int64
	// StackInuse is bytes in stack spans.
	StackInuse int64
	// Sys is total virtual memory obtained from the OS.
	Sys int64
	// HeapObjects is the number of live heap objects.
	HeapObjects uint64
	// NumGC is the number of completed GC cycles.
	NumGC uint32
}

// memSampleNames is the fixed runtime/metrics sample set Memory reads.
// Index order matters — Memory indexes into the read results positionally.
var memSampleNames = []string{
	"/memory/classes/heap/objects:bytes",  // 0: MemStats.HeapAlloc
	"/memory/classes/heap/unused:bytes",   // 1: HeapInuse = objects + unused
	"/memory/classes/heap/free:bytes",     // 2: HeapIdle = free + released
	"/memory/classes/heap/released:bytes", // 3: MemStats.HeapReleased
	"/memory/classes/heap/stacks:bytes",   // 4: MemStats.StackInuse
	"/memory/classes/total:bytes",         // 5: MemStats.Sys
	"/gc/heap/objects:objects",            // 6: MemStats.HeapObjects
	"/gc/cycles/total:gc-cycles",          // 7: MemStats.NumGC
}

// Memory returns a detailed memory stats snapshot.
//
// Sourced from runtime/metrics, NOT runtime.ReadMemStats: ReadMemStats
// stops the world, and with the stats collector calling this every
// broadcast tick the pauses measured 18-512ms per call on an
// oversubscribed host — long enough to stall Raft heartbeat processing
// past the election timeout and flap leadership. metrics.Read takes no
// stop-the-world and reads the same accounting. Field equivalences per
// the runtime/metrics documentation.
func Memory() MemoryStats {
	samples := make([]metrics.Sample, len(memSampleNames))
	for i := range samples {
		samples[i].Name = memSampleNames[i]
	}
	metrics.Read(samples)
	v := func(i int) uint64 {
		if samples[i].Value.Kind() == metrics.KindUint64 {
			return samples[i].Value.Uint64()
		}
		return 0
	}
	heapObjects, heapUnused := v(0), v(1)
	heapFree, heapReleased := v(2), v(3)
	stacks, total := v(4), v(5)

	rss := peakRSS()

	//nolint:gosec // G115: memory stats are always positive and well within int64 range
	return MemoryStats{
		Inuse:        int64(heapObjects + heapUnused + stacks),
		RSS:          rss,
		HeapAlloc:    int64(heapObjects),
		HeapInuse:    int64(heapObjects + heapUnused),
		HeapIdle:     int64(heapFree + heapReleased),
		HeapReleased: int64(heapReleased),
		StackInuse:   int64(stacks),
		Sys:          int64(total),
		HeapObjects:  v(6),
		NumGC:        uint32(v(7)),
	}
}

func peakRSS() int64 {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}
	rss := rusage.Maxrss
	// macOS reports Maxrss in bytes; Linux reports in KB.
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	return rss
}

func getrusageTimes() (user, sys time.Duration) {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0, 0
	}
	user = time.Duration(rusage.Utime.Nano())
	sys = time.Duration(rusage.Stime.Nano())
	return user, sys
}
