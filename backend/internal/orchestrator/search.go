package orchestrator

import (
	"cmp"
	"context"
	"iter"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
)

// Search delegates to the query engine registered under the given key.
// If key is empty, uses "default".
func (o *Orchestrator) Search(ctx context.Context, key string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	key = cmp.Or(key, "default")

	store := o.stores[key]
	if store == nil {
		if len(o.stores) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := store.Query.Search(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchThenFollow delegates to the query engine's SearchThenFollow method.
func (o *Orchestrator) SearchThenFollow(ctx context.Context, key string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	key = cmp.Or(key, "default")

	store := o.stores[key]
	if store == nil {
		if len(o.stores) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := store.Query.SearchThenFollow(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchWithContext delegates to the query engine's SearchWithContext method.
func (o *Orchestrator) SearchWithContext(ctx context.Context, key string, q query.Query) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	key = cmp.Or(key, "default")

	store := o.stores[key]
	if store == nil {
		if len(o.stores) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := store.Query.SearchWithContext(ctx, q)
	return seq, nextToken, nil
}

// Explain returns the query execution plan without executing the query.
func (o *Orchestrator) Explain(ctx context.Context, key string, q query.Query) (*query.QueryPlan, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	key = cmp.Or(key, "default")

	store := o.stores[key]
	if store == nil {
		if len(o.stores) == 0 {
			return nil, ErrNoQueryEngines
		}
		return nil, ErrUnknownRegistry
	}

	return store.Query.Explain(ctx, q)
}
