package system_test

// ParseSizeOrPercent is replicated-and-resolved-per-node like ParseSize,
// so its meaning is pinned the same way: these fail loudly if a canonical
// expression ever drifts. The percentage grammar is
// what makes volume-scaled defaults typeable (docs/product-defaults-policy-
// design.md, decision 6), so its rejects are contracts too.

import (
	"strings"
	"testing"

	"gastrolog/internal/system"
)

func TestParseSizeOrPercentPinsCanonicalExpressions(t *testing.T) {
	const volume = uint64(400) << 30 // 400 GiB
	cases := map[string]uint64{
		// Percentages resolve against the volume; fractional allowed.
		"10%":    40 << 30,
		"3%":     12 << 30,
		"2.5%":   10 << 30,
		"100%":   400 << 30,
		"0.5%":   2 << 30,
		" 10 % ": 40 << 30, // whitespace-tolerant, exactly like ParseSize
		"10 %":   40 << 30,
		// Absolute sizes ignore the volume — plain ParseSize grammar.
		"10GB":  10_000_000_000,
		"10GiB": 10 << 30,
		"5 GiB": 5 << 30,
		"1024":  1024,
	}
	for expr, want := range cases {
		v, err := system.ParseSizeOrPercent(expr)
		if err != nil {
			t.Errorf("ParseSizeOrPercent(%q): %v", expr, err)
			continue
		}
		if got := v.Resolve(volume); got != want {
			t.Errorf("ParseSizeOrPercent(%q).Resolve(400GiB) = %d, want %d", expr, got, want)
		}
		if v.IsPercent() != strings.Contains(expr, "%") {
			t.Errorf("ParseSizeOrPercent(%q).IsPercent() = %v, wrong discriminant", expr, v.IsPercent())
		}
	}
}

func TestParseSizeOrPercentRejectsNonsense(t *testing.T) {
	for _, expr := range []string{
		"", // empty is NOT a valid expression (unset is the caller's rule)
		"   ",
		"150%", // more free space than the volume has
		"100.1%",
		"-5%",
		"%", // no number
		"10%%",
		"10 %% ",
		"abc%",
		"NaN%",
		"max(10%, 10GiB)", // the old formula: not typeable, not parseable
		"gigabytes-please",
	} {
		if _, err := system.ParseSizeOrPercent(expr); err == nil {
			t.Errorf("ParseSizeOrPercent(%q) accepted, want error", expr)
		}
	}
}

func TestSizeOrPercentResolveEdges(t *testing.T) {
	pct, err := system.ParseSizeOrPercent("10%")
	if err != nil {
		t.Fatal(err)
	}
	// Percent of a 0-byte volume is 0 — never a fabricated threshold.
	if got := pct.Resolve(0); got != 0 {
		t.Fatalf("10%% of a 0-byte volume = %d, want 0", got)
	}
	abs, err := system.ParseSizeOrPercent("3GiB")
	if err != nil {
		t.Fatal(err)
	}
	if got := abs.Resolve(0); got != 3<<30 {
		t.Fatalf("an absolute size must not scale with the volume: got %d", got)
	}
	// IsZero sees through both forms; a non-zero value is not zero.
	for expr, wantZero := range map[string]bool{"0": true, "0%": true, "0GB": true, "1%": false, "1": false} {
		v, err := system.ParseSizeOrPercent(expr)
		if err != nil {
			t.Fatalf("ParseSizeOrPercent(%q): %v", expr, err)
		}
		if v.IsZero() != wantZero {
			t.Errorf("ParseSizeOrPercent(%q).IsZero() = %v, want %v", expr, v.IsZero(), wantZero)
		}
	}
}

func TestResolveSizeOrPercentUnsetYieldsDefault(t *testing.T) {
	const volume = uint64(100) << 30
	// Unset resolves the default expression — here a percentage default.
	got, err := system.ResolveSizeOrPercent("", volume, "10%")
	if err != nil || got != 10<<30 {
		t.Fatalf(`ResolveSizeOrPercent("", 100GiB, "10%%") = %d, %v; want 10GiB, nil`, got, err)
	}
	got, err = system.ResolveSizeOrPercent("  ", volume, "3GiB")
	if err != nil || got != 3<<30 {
		t.Fatalf("whitespace is unset: got %d, %v", got, err)
	}
	// A set expression wins over the default.
	got, err = system.ResolveSizeOrPercent("50%", volume, "10%")
	if err != nil || got != 50<<30 {
		t.Fatalf("explicit 50%% = %d, %v; want 50GiB, nil", got, err)
	}
	// A malformed expression surfaces an error, never the default silently.
	if _, err := system.ResolveSizeOrPercent("150%", volume, "10%"); err == nil {
		t.Fatal("ResolveSizeOrPercent should surface a parse error, not the default")
	}
	// A malformed DEFAULT is a programming error and surfaces too.
	if _, err := system.ResolveSizeOrPercent("", volume, "150%"); err == nil {
		t.Fatal("a default that cannot parse must surface as an error")
	}
}
