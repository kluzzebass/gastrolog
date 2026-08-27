package lookup

import (
	"context"
	"sync/atomic"
)

// MaxOutboundPerQuery bounds the number of cache-missing outbound requests one
// query may trigger. Lookup keys come from record fields, so an unbounded query
// over a high-cardinality field would turn one search into a request per
// distinct value against whatever endpoint the operator wired up.
const MaxOutboundPerQuery = 512

type outboundBudgetKey struct{}

type outboundBudget struct {
	remaining atomic.Int64
}

// WithOutboundBudget returns a context carrying a fresh outbound-request
// budget, shared by every lookup performed under it. Call it once per query.
func WithOutboundBudget(ctx context.Context, limit int) context.Context {
	b := &outboundBudget{}
	b.remaining.Store(int64(limit))
	return context.WithValue(ctx, outboundBudgetKey{}, b)
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

// spendOutbound claims one outbound request from ctx's budget. A context with
// no budget is unbounded: single-shot paths such as a configuration test have
// nothing to amplify.
func spendOutbound(ctx context.Context) bool {
	b, ok := ctx.Value(outboundBudgetKey{}).(*outboundBudget)
	if !ok {
		return true
	}
	return b.remaining.Add(-1) >= 0
}
