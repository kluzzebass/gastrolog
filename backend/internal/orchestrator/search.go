package orchestrator

import (
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

	if key == "" {
		key = "default"
	}

	qe, ok := o.queries[key]
	if !ok {
		if len(o.queries) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := qe.Search(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchThenFollow delegates to the query engine's SearchThenFollow method.
func (o *Orchestrator) SearchThenFollow(ctx context.Context, key string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if key == "" {
		key = "default"
	}

	qe, ok := o.queries[key]
	if !ok {
		if len(o.queries) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := qe.SearchThenFollow(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchWithContext delegates to the query engine's SearchWithContext method.
func (o *Orchestrator) SearchWithContext(ctx context.Context, key string, q query.Query) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if key == "" {
		key = "default"
	}

	qe, ok := o.queries[key]
	if !ok {
		if len(o.queries) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := qe.SearchWithContext(ctx, q)
	return seq, nextToken, nil
}
