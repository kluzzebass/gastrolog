package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
	"iter"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
)

// Search delegates to the query engine registered under the given key.
// If key is empty, uses "default".
func (o *Orchestrator) Search(ctx context.Context, key glid.GLID, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	vault := o.vaults[key]
	if vault == nil {
		if len(o.vaults) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}
	if err := vaultReplicationReadinessErr(key, vault); err != nil {
		return nil, nil, err
	}

	qe := vault.QueryEngine()
	if qe == nil {
		return nil, nil, ErrNoQueryEngines
	}
	seq, nextToken := qe.Search(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchThenFollow delegates to the query engine's SearchThenFollow method.
func (o *Orchestrator) SearchThenFollow(ctx context.Context, key glid.GLID, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	vault := o.vaults[key]
	if vault == nil {
		if len(o.vaults) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}
	if err := vaultReplicationReadinessErr(key, vault); err != nil {
		return nil, nil, err
	}

	qe := vault.QueryEngine()
	if qe == nil {
		return nil, nil, ErrNoQueryEngines
	}
	seq, nextToken := qe.SearchThenFollow(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchWithContext delegates to the query engine's SearchWithContext method.
func (o *Orchestrator) SearchWithContext(ctx context.Context, key glid.GLID, q query.Query) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	vault := o.vaults[key]
	if vault == nil {
		if len(o.vaults) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}
	if err := vaultReplicationReadinessErr(key, vault); err != nil {
		return nil, nil, err
	}

	qe := vault.QueryEngine()
	if qe == nil {
		return nil, nil, ErrNoQueryEngines
	}
	seq, nextToken := qe.SearchWithContext(ctx, q)
	return seq, nextToken, nil
}

// Explain returns the query execution plan without executing the query.
func (o *Orchestrator) Explain(ctx context.Context, key glid.GLID, q query.Query) (*query.QueryPlan, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	vault := o.vaults[key]
	if vault == nil {
		if len(o.vaults) == 0 {
			return nil, ErrNoQueryEngines
		}
		return nil, ErrUnknownRegistry
	}
	if err := vaultReplicationReadinessErr(key, vault); err != nil {
		return nil, err
	}

	qe := vault.QueryEngine()
	if qe == nil {
		return nil, ErrNoQueryEngines
	}
	return qe.Explain(ctx, q)
}
