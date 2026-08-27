package lookup

import (
	"context"
	"sync/atomic"
)

// MaxOutboundPerQuery bounds the number of cache-missing outbound requests one
// query may trigger on one node. Lookup keys come from record fields, so an
// unbounded query over a high-cardinality field would turn one search into a
// request per distinct value against whatever endpoint the operator wired up.
//
// The budget is per node: a query that fans out to N nodes may cost N times
// this at the endpoint, because each node enriches the records it scanned.
const MaxOutboundPerQuery = 512

type outboundBudgetKey struct{}

type outboundBudget struct {
	limit     int
	remaining atomic.Int64
	reported  atomic.Bool
}

// WithOutboundBudget returns a context carrying a fresh outbound-request
// budget, shared by every lookup performed under it. Call it once per query —
// EnsureOutboundBudget is the safe form when a path may already be covered.
func WithOutboundBudget(ctx context.Context, limit int) context.Context {
	b := &outboundBudget{limit: limit}
	b.remaining.Store(int64(limit))
	return context.WithValue(ctx, outboundBudgetKey{}, b)
}

// EnsureOutboundBudget installs the default budget unless ctx already carries
// one, so nesting query entry points cannot hand a query a second allowance.
func EnsureOutboundBudget(ctx context.Context) context.Context {
	if _, ok := OutboundRemaining(ctx); ok {
		return ctx
	}
	return WithOutboundBudget(ctx, MaxOutboundPerQuery)
}

// OutboundRemaining reports how much of ctx's budget is left, and whether ctx
// carries one at all.
func OutboundRemaining(ctx context.Context) (int, bool) {
	b, ok := ctx.Value(outboundBudgetKey{}).(*outboundBudget)
	if !ok {
		return 0, false
	}
	return int(max(b.remaining.Load(), 0)), true
}

// spendOutbound claims one outbound request from ctx's budget. It reports
// whether the request may proceed, and whether this is the first refusal under
// this budget so exhaustion is announced once per query rather than once per
// record. A context with no budget is unbounded: single-shot paths such as a
// configuration test have nothing to amplify.
func spendOutbound(ctx context.Context) (allowed, firstRefusal bool) {
	b, ok := ctx.Value(outboundBudgetKey{}).(*outboundBudget)
	if !ok {
		return true, false
	}
	if b.remaining.Add(-1) >= 0 {
		return true, false
	}
	return false, b.reported.CompareAndSwap(false, true)
}

// outboundLimit reports the budget ctx was given, and whether it has one.
func outboundLimit(ctx context.Context) (int, bool) {
	b, ok := ctx.Value(outboundBudgetKey{}).(*outboundBudget)
	if !ok {
		return 0, false
	}
	return b.limit, true
}
