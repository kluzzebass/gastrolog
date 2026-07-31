package server

import (
	"cmp"
	"context"
	"gastrolog/internal/glid"
	"iter"
	"slices"
	"sync"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
)

func forwardSearchRequestFromTarget(t searchPartitionTarget, queryExpr string, resumeToken []byte) *apiv1.ForwardSearchRequest {
	req := &apiv1.ForwardSearchRequest{
		VaultId:              t.vaultID.ToProto(),
		Query:                queryExpr,
		ResumeToken:          resumeToken,
		SearchPipelineChunks: t.pipelineChunks,
	}
	if len(t.sealedChunkIDs) > 0 {
		req.SealedChunkIds = make([][]byte, len(t.sealedChunkIDs))
		for i, id := range t.sealedChunkIDs {
			req.SealedChunkIds[i] = id[:]
		}
	}
	return req
}

// collectPartitionRemote opens streaming ForwardSearch RPCs for remote holder
// partitions and returns a merged sorted iterator, combined histogram, and
// the set of remote vaults fanned out to (the contributor set — see
// collectRemote).
func (s *QueryServer) collectPartitionRemote(
	ctx context.Context,
	q query.Query,
	targets []searchPartitionTarget,
	remoteTokens map[glid.GLID][]byte,
) (iter.Seq2[chunk.Record, error], []*apiv1.HistogramBucket, []glid.GLID) {
	if s.remoteSearcher == nil || len(targets) == 0 {
		return nil, nil, nil
	}
	if remoteTokens == nil {
		remoteTokens = make(map[glid.GLID][]byte)
	}

	queryExpr := q.String()

	type partitionStream struct {
		records <-chan []*apiv1.ExportRecord
		errCh   <-chan error
		vaultID glid.GLID
	}
	var streams []partitionStream
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, target := range targets {
		wg.Go(func() {
			req := forwardSearchRequestFromTarget(target, queryExpr, remoteTokens[target.vaultID])
			// Remote resume tokens are not propagated (see collectRemote).
			recCh, _, eCh, _, _ := s.remoteSearcher.SearchStream(ctx, target.nodeID, req)
			mu.Lock()
			streams = append(streams, partitionStream{
				records: recCh,
				errCh:   eCh,
				vaultID: target.vaultID,
			})
			mu.Unlock()
		})
	}
	wg.Wait()

	if len(streams) == 0 {
		return nil, nil, nil
	}

	// Contributor set: the remote holder-partition vaults fanned out to.
	contributingVaults := make([]glid.GLID, 0, len(streams))
	for _, ps := range streams {
		contributingVaults = append(contributingVaults, ps.vaultID)
	}
	slices.SortFunc(contributingVaults, func(a, b glid.GLID) int {
		return cmp.Compare(a.String(), b.String())
	})

	var iters []iter.Seq2[chunk.Record, error]
	for _, ps := range streams {
		iters = append(iters, channelToIter(ps.records, ps.errCh))
	}
	if len(iters) == 1 {
		return iters[0], nil, contributingVaults
	}
	return kWayMerge(iters, q.OrderBy, q.Reverse()), nil, contributingVaults
}

// searchPartitionTargets runs local holder-partition searches and merges them
// when multiple vault slices are assigned to this node.
func (s *QueryServer) searchPartitionTargets(
	ctx context.Context,
	q query.Query,
	resume *query.ResumeToken,
	targets []searchPartitionTarget,
) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken) {
	empty := func(yield func(chunk.Record, error) bool) {}
	nilToken := func() *query.ResumeToken { return nil }
	if len(targets) == 0 {
		return empty, nilToken
	}
	if len(targets) == 1 {
		t := targets[0]
		eng, err := s.orch.HolderQueryEngineForVault(t.vaultID, holderScopeFromTarget(t))
		if err != nil {
			return func(yield func(chunk.Record, error) bool) {
				yield(chunk.Record{}, err)
			}, nilToken
		}
		if eng == nil {
			return empty, nilToken
		}
		return eng.Search(ctx, q, resume)
	}

	type localStream struct {
		iter     iter.Seq2[chunk.Record, error]
		getToken func() *query.ResumeToken
	}
	streams := make([]localStream, 0, len(targets))
	for _, t := range targets {
		eng, err := s.orch.HolderQueryEngineForVault(t.vaultID, holderScopeFromTarget(t))
		if err != nil {
			return func(yield func(chunk.Record, error) bool) {
				yield(chunk.Record{}, err)
			}, nilToken
		}
		if eng == nil {
			continue
		}
		it, getToken := eng.Search(ctx, q, resume)
		streams = append(streams, localStream{iter: it, getToken: getToken})
	}
	if len(streams) == 0 {
		return empty, nilToken
	}
	if len(streams) == 1 {
		return streams[0].iter, streams[0].getToken
	}

	var iters []iter.Seq2[chunk.Record, error]
	for _, ls := range streams {
		iters = append(iters, ls.iter)
	}
	merged := kWayMerge(iters, q.OrderBy, q.Reverse())
	getToken := func() *query.ResumeToken {
		// Highwater-based pagination is authoritative across partitions;
		// return the first stream's token as a hint for local positions.
		return streams[0].getToken()
	}
	return merged, getToken
}

// shouldUseDistributedSealedSearch gates RF-partitioned sealed search.
//
// Cross-node k-way merge already exists for multi-vault search: one ordered
// stream per vault from its placement leader, merged remotely then combined
// with local via mergeIterators (global limit at the coordinator). RF
// partitioning changes the fan-out unit for a single vault from one leader
// stream to N holder streams — multiplying RPCs and chunk-selection work for
// the common single-vault case without reducing scan cost for top-N queries.
// Re-enable once holder fan-out is a net win on the same merge path.
func shouldUseDistributedSealedSearch(_ query.Query) bool {
	return false
}

// usesDistributedSearchTargets reports whether any vault is searched from
// more than one holder node.
func usesDistributedSearchTargets(targets []searchPartitionTarget) bool {
	perVault := make(map[glid.GLID]int)
	for _, t := range targets {
		perVault[t.vaultID]++
		if perVault[t.vaultID] > 1 {
			return true
		}
	}
	return false
}
