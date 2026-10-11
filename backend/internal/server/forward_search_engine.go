package server

import (
	"context"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

// ForwardSearchEngine resolves the query engine for a ForwardSearch RPC: the
// vault's leader engine on this node, or nil when the request names no vault.
func ForwardSearchEngine(o *orchestrator.Orchestrator, req *apiv1.ForwardSearchRequest) (*query.Engine, error) {
	vaultID := glid.FromBytes(req.GetVaultId())
	if vaultID.IsZero() {
		return nil, nil
	}
	return o.LeaderQueryEngineForVault(vaultID)
}

// pageHistogramBuckets is the bucket count of a search's page histogram. Every
// node computes its vaults' share on the same grid, so the coordinator sums the
// shares bucket by bucket.
const pageHistogramBuckets = 50

// ForwardSearchHistogram answers a histogram_only ForwardSearch: the vault's
// page histogram, computed exactly as the coordinator computes it for the
// vaults it leads itself.
func ForwardSearchHistogram(ctx context.Context, eng *query.Engine, q query.Query) []*apiv1.HistogramBucket {
	return HistogramToProto(eng.ComputeSearchPageHistogram(ctx, q, pageHistogramBuckets))
}
