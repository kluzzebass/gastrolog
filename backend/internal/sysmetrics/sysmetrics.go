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

// MemoryInuse returns the memory actively in use by the Go runtime, in
// bytes. This is HeapInuse (live heap spans) plus StackInuse (goroutine
// stacks), excluding virtual address space reserved but not committed.
func MemoryInuse() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapInuse + m.StackInuse)
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
