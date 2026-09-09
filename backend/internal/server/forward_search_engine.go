package server

import (
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

// ForwardSearchIncludesHistogram reports whether a ForwardSearch handler should
// run the ITSI histogram pre-pass before streaming records. A filtered search
// already pays a full record scan, so the pre-pass would only duplicate it.
func ForwardSearchIncludesHistogram(q query.Query) bool {
	return q.BoolExpr == nil
}
