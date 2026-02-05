package repl

import (
	"context"
	"iter"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

// DirectClient implements Client by directly calling the orchestrator.
// This is used for embedded mode where everything runs in-process.
type DirectClient struct {
	orch *orchestrator.Orchestrator
}

var _ Client = (*DirectClient)(nil)

// NewDirectClient creates a client that talks directly to the orchestrator.
func NewDirectClient(orch *orchestrator.Orchestrator) *DirectClient {
	return &DirectClient{orch: orch}
}

func (c *DirectClient) Search(ctx context.Context, store string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	return c.orch.Search(ctx, store, q, resume)
}

func (c *DirectClient) Explain(ctx context.Context, store string, q query.Query) (*query.QueryPlan, error) {
	return c.orch.Explain(ctx, store, q)
}

func (c *DirectClient) ListStores() []string {
	return c.orch.ListStores()
}

func (c *DirectClient) ChunkManager(store string) ChunkReader {
	cm := c.orch.ChunkManager(store)
	if cm == nil {
		return nil
	}
	return cm
}

func (c *DirectClient) IndexManager(store string) IndexReader {
	im := c.orch.IndexManager(store)
	if im == nil {
		return nil
	}
	return &directIndexReader{im: im}
}

func (c *DirectClient) IsRunning() bool {
	return c.orch.IsRunning()
}

// Analyzer creates an analyzer for the given store.
func (c *DirectClient) Analyzer(store string) AnalyzerClient {
	cm := c.orch.ChunkManager(store)
	im := c.orch.IndexManager(store)
	if cm == nil || im == nil {
		return nil
	}
	return analyzer.New(cm, im)
}

// directIndexReader wraps an index.IndexManager as IndexReader.
type directIndexReader struct {
	im index.IndexManager
}

func (r *directIndexReader) IndexesComplete(id chunk.ChunkID) (bool, error) {
	return r.im.IndexesComplete(id)
}

func (r *directIndexReader) OpenTokenIndex(id chunk.ChunkID) (TokenIndex, error) {
	idx, err := r.im.OpenTokenIndex(id)
	if err != nil {
		return nil, err
	}
	return &directTokenIndex{idx: idx}, nil
}

// directTokenIndex wraps index.Index[index.TokenIndexEntry] as TokenIndex.
type directTokenIndex struct {
	idx *index.Index[index.TokenIndexEntry]
}

func (t *directTokenIndex) Entries() []TokenIndexEntry {
	entries := t.idx.Entries()
	result := make([]TokenIndexEntry, len(entries))
	for i, e := range entries {
		result[i] = TokenIndexEntry{
			Token:     e.Token,
			Positions: e.Positions,
		}
	}
	return result
}
