// Per-vault W-of-N policy resolver for the fan-out data-plane epic
// (gastrolog-2ujjh / gastrolog-4xdvm).
//
// The policy is a per-vault choice (Full / MinusOne / Quorum / One)
// that resolves to a concrete W integer at write time against the
// active chunk's Receiving snapshot size. Resolution lives here so
// every caller (orchestrator's fan-out write, integration tests, CLI
// preview, UI display) gets the same number.

package system

import (
	"errors"
	"fmt"
)

var errEmptyReceivingSnapshot = errors.New("W-of-N: empty Receiving snapshot")

// WOfNPolicy selects the durability tier for a vault. The value
// resolves to a concrete W integer at write time against the active
// chunk's Receiving snapshot size.
type WOfNPolicy string

const (
	// WOfNPolicyFull demands every Receiving member ack before a
	// write is durable. The default for high-durability vaults
	// (compliance, audit logs).
	WOfNPolicyFull WOfNPolicy = "full"
	// WOfNPolicyMinusOne tolerates one straggler. W = max(1, N − 1).
	// Useful when the operator wants high durability but accepts a
	// single slow / dead replica without write stalls.
	WOfNPolicyMinusOne WOfNPolicy = "minus-one"
	// WOfNPolicyQuorum demands a majority ack: W = ceil(N / 2).
	// Balances durability and throughput; the default for most
	// FanOut vaults.
	WOfNPolicyQuorum WOfNPolicy = "quorum"
	// WOfNPolicyOne accepts any single ack as durable. Maximum
	// throughput, minimum durability. Suited to high-volume metrics
	// firehoses where occasional lost records are tolerable and
	// throughput dominates.
	WOfNPolicyOne WOfNPolicy = "one"
)

// IsValid reports whether p is one of the canonical policy values.
func (p WOfNPolicy) IsValid() bool {
	switch p {
	case WOfNPolicyFull, WOfNPolicyMinusOne, WOfNPolicyQuorum, WOfNPolicyOne:
		return true
	}
	return false
}

// String returns the canonical wire form. Implements fmt.Stringer.
func (p WOfNPolicy) String() string { return string(p) }

// Resolve returns the concrete W integer for the given Receiving
// snapshot size. Always returns at least 1 — a 0-ack write is never
// durable, even when the Receiving set is empty by accident. Returns
// an error iff the policy value is unrecognized.
func (p WOfNPolicy) Resolve(n int) (int, error) {
	if n <= 0 {
		// Caller bug / config error — preserved as a defensive guard
		// rather than silent fallback. Production callers MUST
		// validate the Receiving snapshot is non-empty before
		// resolving the policy.
		return 0, errEmptyReceivingSnapshot
	}
	switch p {
	case WOfNPolicyFull, "":
		// Empty policy defaults to Full. Newly-created vaults
		// without an explicit policy retain wait-for-all semantics,
		// matching pre-fan-out durability behavior.
		return n, nil
	case WOfNPolicyMinusOne:
		if n == 1 {
			return 1, nil
		}
		return n - 1, nil
	case WOfNPolicyQuorum:
		return (n / 2) + 1, nil
	case WOfNPolicyOne:
		return 1, nil
	default:
		return 0, fmt.Errorf("W-of-N: unknown policy %q", p)
	}
}

// ResolveWOfNPolicy is a free-function alias for callers that have
// the policy as a plain string and want one-line resolution. Returns
// the same (W, err) tuple as the method.
func ResolveWOfNPolicy(policy string, n int) (int, error) {
	return WOfNPolicy(policy).Resolve(n)
}
