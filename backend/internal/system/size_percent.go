package system

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Volume-relative size expressions: a threshold on a volume can be an
// absolute size ("10GB") or a percentage of that volume ("10%", "2.5%").
// The percentage form exists so that volume-scaled defaults are TYPEABLE —
// a default must be a value the operator could have entered in the same
// field, and no field grammar can express max(fraction·volume, hardBytes)
// (operator directive, recorded in docs/product-defaults-policy-design.md).
//
// Like ParseSize / ParseDuration, this is the ONLY parser for the grammar:
// no call site inspects "%" itself. Fields where a percentage does not
// compose (per-vault budgets sharing a volume) keep using ParseSize.

// SizeOrPercent is the discriminated result of ParseSizeOrPercent: either an
// absolute byte count or a percentage to be resolved against a volume size.
type SizeOrPercent struct {
	percent bool
	pct     float64 // 0–100, meaningful when percent
	bytes   uint64  // meaningful when !percent
}

// IsPercent reports whether the expression was a percentage of the volume.
func (v SizeOrPercent) IsPercent() bool { return v.percent }

// IsZero reports whether the expression names a zero threshold ("0", "0GB",
// "0%") regardless of volume size. Callers that reject explicit zeros use
// this rather than resolving against a placeholder volume.
func (v SizeOrPercent) IsZero() bool {
	if v.percent {
		return v.pct == 0
	}
	return v.bytes == 0
}

// Resolve returns the value in bytes against the given volume size. An
// absolute size ignores totalBytes; a percentage of a 0-byte volume is 0.
func (v SizeOrPercent) Resolve(totalBytes uint64) uint64 {
	if !v.percent {
		return v.bytes
	}
	return uint64(float64(totalBytes) * v.pct / 100)
}

// ParseSizeOrPercent parses a size-or-percent expression. Whitespace is
// tolerated anywhere, like ParseSize ("10 %" == "10%"). A percentage must be
// in [0, 100] — free space cannot exceed the volume — and fractional
// percentages ("2.5%") are allowed. Anything without a "%" suffix is a plain
// ParseSize expression.
func ParseSizeOrPercent(s string) (SizeOrPercent, error) {
	// Same normalization as ParseSize: collapse ALL whitespace, so the two
	// grammars tolerate exactly the same sloppiness (gastrolog-etcjdx).
	s = strings.Join(strings.Fields(s), "")
	if s == "" {
		return SizeOrPercent{}, errors.New("empty size string")
	}
	if !strings.HasSuffix(s, "%") {
		bytes, err := ParseSize(s)
		if err != nil {
			return SizeOrPercent{}, err
		}
		return SizeOrPercent{bytes: bytes}, nil
	}
	numStr := strings.TrimSuffix(s, "%")
	if numStr == "" || strings.ContainsAny(numStr, "%") {
		return SizeOrPercent{}, fmt.Errorf("invalid percentage: %q", s)
	}
	pct, err := strconv.ParseFloat(numStr, 64)
	if err != nil || math.IsNaN(pct) {
		return SizeOrPercent{}, fmt.Errorf("invalid percentage: %q", s)
	}
	if pct < 0 || pct > 100 {
		return SizeOrPercent{}, fmt.Errorf("percentage out of range 0–100: %q", s)
	}
	return SizeOrPercent{percent: true, pct: pct}, nil
}

// ResolveSizeOrPercent resolves a size-or-percent expression to bytes against
// a volume size. An empty expression means "unset" and resolves def instead —
// def is a declared constant expression at every call site ("10%"), so a def
// that fails to parse is a programming error and surfaces as one. Follows
// SizeOrDefault's conventions: callers get an error rather than a silent zero.
func ResolveSizeOrPercent(expr string, totalBytes uint64, def string) (uint64, error) {
	if strings.TrimSpace(expr) == "" {
		expr = def
	}
	v, err := ParseSizeOrPercent(expr)
	if err != nil {
		return 0, err
	}
	return v.Resolve(totalBytes), nil
}
