// Package profiling configures runtime sampling for net/http/pprof endpoints.
package profiling

import (
	"log/slog"
	"runtime"
)

// Defaults used by --pprof-debug (dev clusters / incident capture).
const (
	DebugMutexFraction = 5
	DebugBlockRate     = 10_000_000 // one sample per 10ms blocked
)

// Config holds runtime profile sampling rates. Zero values leave profiling off.
type Config struct {
	MutexFraction int // runtime.SetMutexProfileFraction; 1 = all, n = 1/n events
	BlockRate     int // runtime.SetBlockProfileRate; nanoseconds between samples
}

// Setup applies mutex and block profiling. Safe to call with zero values (no-op).
func Setup(logger *slog.Logger, cfg Config) {
	if cfg.MutexFraction != 0 {
		runtime.SetMutexProfileFraction(cfg.MutexFraction)
		if logger != nil {
			logger.Info("mutex profiling enabled", "fraction", cfg.MutexFraction)
		}
	}
	if cfg.BlockRate != 0 {
		runtime.SetBlockProfileRate(cfg.BlockRate)
		if logger != nil {
			logger.Info("block profiling enabled", "rate_ns", cfg.BlockRate)
		}
	}
}

// DebugConfig returns sampling rates for --pprof-debug.
func DebugConfig() Config {
	return Config{
		MutexFraction: DebugMutexFraction,
		BlockRate:     DebugBlockRate,
	}
}
