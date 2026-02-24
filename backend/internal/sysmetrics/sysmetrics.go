// Package sysmetrics tracks process-level CPU and memory usage.
package sysmetrics

import (
	"runtime"
	"sync"
	"syscall"
	"time"
)

var (
	mu       sync.Mutex
	lastWall time.Time
	lastUser time.Duration
	lastSys  time.Duration
	lastCPU  float64
)

func init() {
	now := time.Now()
	utime, stime := getrusageTimes()
	mu.Lock()
	lastWall = now
	lastUser = utime
	lastSys = stime
	mu.Unlock()
}

// CPUPercent returns the process CPU usage as a percentage (0–100+)
// since the last call. Multi-core processes can exceed 100%.
func CPUPercent() float64 {
	now := time.Now()
	utime, stime := getrusageTimes()

	mu.Lock()
	defer mu.Unlock()

	wall := now.Sub(lastWall)
	if wall <= 0 {
		return lastCPU
	}

	cpuDelta := (utime - lastUser) + (stime - lastSys)
	pct := float64(cpuDelta) / float64(wall) * 100.0

	lastWall = now
	lastUser = utime
	lastSys = stime
	lastCPU = pct

	return pct
}

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

// Memory returns a detailed memory stats snapshot.
func Memory() MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	rss := peakRSS()

	//nolint:gosec // G115: memory stats are always positive and well within int64 range
	return MemoryStats{
		Inuse:        int64(m.HeapInuse + m.StackInuse),
		RSS:          rss,
		HeapAlloc:    int64(m.HeapAlloc),
		HeapInuse:    int64(m.HeapInuse),
		HeapIdle:     int64(m.HeapIdle),
		HeapReleased: int64(m.HeapReleased),
		StackInuse:   int64(m.StackInuse),
		Sys:          int64(m.Sys),
		HeapObjects:  m.HeapObjects,
		NumGC:        m.NumGC,
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
