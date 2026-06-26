package profiling

import (
	"log/slog"
	"testing"
)

func TestSetupZeroNoPanic(t *testing.T) {
	t.Parallel()
	Setup(nil, Config{})
	Setup(slog.Default(), Config{MutexFraction: 0, BlockRate: 0})
}

func TestSetupDebugConfig(t *testing.T) {
	t.Parallel()
	cfg := DebugConfig()
	if cfg.MutexFraction != DebugMutexFraction {
		t.Fatalf("MutexFraction = %d, want %d", cfg.MutexFraction, DebugMutexFraction)
	}
	if cfg.BlockRate != DebugBlockRate {
		t.Fatalf("BlockRate = %d, want %d", cfg.BlockRate, DebugBlockRate)
	}
}
