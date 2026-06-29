package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

func TestForwardSearchIncludesHistogram(t *testing.T) {
	t.Parallel()
	last5m := query.Query{Limit: 500}

	full := &apiv1.ForwardSearchRequest{}
	if !ForwardSearchIncludesHistogram(full, last5m) {
		t.Fatal("legacy full-vault forward should include histogram")
	}

	id := chunk.NewChunkID()
	partitioned := &apiv1.ForwardSearchRequest{
		SealedChunkIds: [][]byte{id[:]},
	}
	if ForwardSearchIncludesHistogram(partitioned, last5m) {
		t.Fatal("sealed partition slice should skip histogram")
	}

	leaderSlice := &apiv1.ForwardSearchRequest{
		SearchPipelineChunks: true,
		SealedChunkIds:       [][]byte{id[:]},
	}
	if ForwardSearchIncludesHistogram(leaderSlice, last5m) {
		t.Fatal("leader partition slice should skip histogram")
	}

	filtered := last5m
	filtered.BoolExpr = &querylang.PredicateExpr{Kind: querylang.PredToken, Value: "error"}
	if ForwardSearchIncludesHistogram(full, filtered) {
		t.Fatal("filtered query should skip histogram pre-pass")
	}
}
