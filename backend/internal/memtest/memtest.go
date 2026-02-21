// Package memtest provides shared test helpers for creating memory-backed
// chunk managers, index managers, and query engines. It eliminates the
// boilerplate of wiring up memtoken/memattr/memkv indexers that is
// duplicated across query, orchestrator, and server test files.
package memtest

import (
	"context"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memjson "gastrolog/internal/index/memory/json"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/query"
)

// Store bundles a memory chunk manager, index manager, and query engine.
type Store struct {
	CM chunk.ChunkManager
	IM index.IndexManager
	QE *query.Engine
}

// NewStore creates a memory-backed Store with the given chunk manager config.
// The index manager is wired with token, attr, and kv indexers.
func NewStore(cfg chunkmem.Config) (Store, error) {
	cm, err := chunkmem.NewManager(cfg)
	if err != nil {
		return Store{}, err
	}
	im := newIndexManager(cm)
	qe := query.New(cm, im, nil)
	return Store{CM: cm, IM: im, QE: qe}, nil
}

// MustNewStore is like NewStore but calls t.Fatal on error.
func MustNewStore(t *testing.T, cfg chunkmem.Config) Store {
	t.Helper()
	s, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("memtest.NewStore: %v", err)
	}
	return s
}

// BuildIndexes builds indexes for all sealed chunks.
func BuildIndexes(t *testing.T, cm chunk.ChunkManager, im index.IndexManager) {
	t.Helper()
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		if err := im.BuildIndexes(context.Background(), m.ID); err != nil {
			t.Fatalf("build indexes for %s: %v", m.ID, err)
		}
	}
}

func newIndexManager(cm chunk.ChunkManager) index.IndexManager {
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	jsonIdx := memjson.NewIndexer(cm)
	return indexmem.NewManagerWithJSON(
		[]index.Indexer{tokIdx, attrIdx, kvIdx, jsonIdx},
		tokIdx,
		attrIdx,
		kvIdx,
		jsonIdx,
		nil,
	)
}
