package system

import (
	"strings"
	"time"
)

// Config quantities (sizes, durations) are stored as the operator's own
// expression — "5GiB", "3m" — and resolved here, at the point of use, rather
// than collapsed to a number at ingress. Storing the expression keeps config
// readable at rest and in export, echoes back exactly what was entered, and
// leaves room for values that must be resolved against per-node context.
//
// These are the ONLY resolution entry points for config quantities. No call
// site parses one itself: unit handling and the unset rule live here, so a
// change to either happens in one place.
//
// The parsers (ParseSize, ParseDuration) are deterministic and pinned by
// round-trip tests — that is what makes it safe to replicate the expression
// and resolve it independently on each node.

// SizeOrDefault resolves a size expression to bytes. An empty expression means
// "unset" and yields def; anything else must parse. Callers get an error
// rather than a silent zero, because a size that silently became 0 is the
// unbounded-or-refuse-everything footgun this model exists to avoid.
func SizeOrDefault(expr string, def uint64) (uint64, error) {
	if strings.TrimSpace(expr) == "" {
		return def, nil
	}
	return ParseSize(expr)
}

// DurationOrDefault resolves a duration expression. Empty means "unset" and
// yields def; anything else must parse.
func DurationOrDefault(expr string, def time.Duration) (time.Duration, error) {
	if strings.TrimSpace(expr) == "" {
		return def, nil
	}
	return ParseDuration(expr)
}

// IsQuantityUnset reports whether an operator left a quantity unset. Empty (and
// whitespace) is unset; "0" is NOT unset — it is an explicit zero, which most
// fields reject, and conflating the two is what let unbounded vaults exist.
func IsQuantityUnset(expr string) bool {
	return strings.TrimSpace(expr) == ""
}
