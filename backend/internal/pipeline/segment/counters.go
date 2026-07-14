package segment

import "sync/atomic"

// Cumulative process-wide open counters, exposed so tests can assert
// open-count behavior — e.g. reverse cursor reads must not re-open and
// re-verify a segment per record (gastrolog-54mjat) — instead of timing.
var (
	opens       atomic.Uint64
	mappedOpens atomic.Uint64
)

// Opens returns the cumulative number of Open calls (full-verify opens).
func Opens() uint64 { return opens.Load() }

// MappedOpens returns the cumulative number of OpenMapped calls.
func MappedOpens() uint64 { return mappedOpens.Load() }
