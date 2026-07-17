package system_test

// The config-quantity model replicates the operator's expression and resolves
// it independently on each node (gastrolog-etcjdx). That is only safe if the
// parsers are deterministic and stable: these pins fail loudly if a canonical
// expression's meaning ever drifts, which is the exact risk the old
// numeric-at-rest rule guarded against and this replaces.

import (
	"testing"
	"time"

	"gastrolog/internal/system"
)

func TestParseSizePinsCanonicalExpressions(t *testing.T) {
	cases := map[string]uint64{
		"0":     0,
		"1":     1,
		"1KB":   1_000,
		"1KiB":  1024,
		"64MB":  64_000_000,
		"64MiB": 64 << 20,
		"1GB":   1_000_000_000,
		"1GiB":  1 << 30,
		"50GB":  50_000_000_000,
		"100TiB": 100 << 40,
		"1PiB":  1 << 50,
		"1EiB":  1 << 60,
	}
	for expr, want := range cases {
		got, err := system.ParseSize(expr)
		if err != nil {
			t.Errorf("ParseSize(%q): %v", expr, err)
			continue
		}
		if got != want {
			t.Errorf("ParseSize(%q) = %d, want %d — a canonical size expression changed meaning", expr, got, want)
		}
	}
}

func TestParseDurationPinsCanonicalExpressions(t *testing.T) {
	cases := map[string]time.Duration{
		"0":  0,
		"1s": time.Second,
		"1m": time.Minute,
		"5m": 5 * time.Minute,
		"1h": time.Hour,
		"2h": 2 * time.Hour,
		"7d": 7 * 24 * time.Hour,
	}
	for expr, want := range cases {
		got, err := system.ParseDuration(expr)
		if err != nil {
			t.Errorf("ParseDuration(%q): %v", expr, err)
			continue
		}
		if got != want {
			t.Errorf("ParseDuration(%q) = %v, want %v — a canonical duration expression changed meaning", expr, got, want)
		}
	}
}

func TestSizeOrDefaultUnsetYieldsDefault(t *testing.T) {
	got, err := system.SizeOrDefault("", 1<<30)
	if err != nil || got != 1<<30 {
		t.Fatalf("SizeOrDefault(\"\", 1GiB) = %d, %v; want 1GiB, nil", got, err)
	}
	got, err = system.SizeOrDefault("  ", 42)
	if err != nil || got != 42 {
		t.Fatalf("whitespace is unset: got %d, %v", got, err)
	}
	if _, err := system.SizeOrDefault("nonsense", 1); err == nil {
		t.Fatal("SizeOrDefault should surface a parse error, not the default")
	}
}

func TestIsQuantityUnsetDistinguishesEmptyFromZero(t *testing.T) {
	if !system.IsQuantityUnset("") || !system.IsQuantityUnset("  ") {
		t.Fatal("empty/whitespace must be unset")
	}
	if system.IsQuantityUnset("0") {
		t.Fatal(`"0" must NOT be unset — it is an explicit zero, which most fields reject`)
	}
}
