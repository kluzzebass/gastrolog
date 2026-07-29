package server

import (
	"cmp"
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"iter"
	"slices"
	"sync"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"
)

// collectRemote opens streaming ForwardSearch RPCs to all remote vaults and
// returns a merged sorted iterator over their records, the combined
// histogram, and the set of remote vaults this search fanned out to. The
// iterator performs a k-way merge — at most one record per remote vault is
// held in memory at any time.
//
// The third return is the per-vault stream-health signal (gastrolog-20lrg):
// the remote vaults contributing to this merged search. Because a remote
// stream failure aborts the whole search (mergeIterators → mapSearchError,
// the same fail-on-remote-failure policy the pipeline path now shares), any
// successfully-returned response has been assembled from EVERY vault in this
// set — so the fanned-out set is exactly the contributor set. nil when there
// are no remote vaults in scope.
func (s *QueryServer) collectRemote(ctx context.Context, q query.Query, remoteTokens map[glid.GLID][]byte) (iter.Seq2[chunk.Record, error], []*apiv1.HistogramBucket, []glid.GLID) {
	if s.remoteSearcher == nil || s.cfgStore == nil {
		return nil, nil, nil
	}
	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	byNode := s.remoteVaultsByNode(ctx, selectedVaults)
	if len(byNode) == 0 {
		return nil, nil, nil
	}

	queryExpr := q.String()
	if remoteTokens == nil {
		remoteTokens = make(map[glid.GLID][]byte)
	}

	// Fan out streaming RPCs concurrently — one per remote vault.
	type vaultStream struct {
		records      <-chan []*apiv1.ExportRecord
		errCh        <-chan error
		getHistogram func() []*apiv1.HistogramBucket
		vaultID      glid.GLID
	}
	var streams []vaultStream
	var allHist []*apiv1.HistogramBucket
	var mu sync.Mutex
	var wg sync.WaitGroup

	for nodeID, vaultIDs := range byNode {
		for _, vid := range vaultIDs {
			wg.Go(func() {
				// Remote opaque resume tokens are deliberately not propagated
				// (the merge-level highwater drives pagination — see
				// searchDirect), so the token getter is dropped here.
				recCh, _, eCh, _, getHist := s.remoteSearcher.SearchStream(ctx, nodeID, &apiv1.ForwardSearchRequest{
					VaultId:     vid.ToProto(),
					Query:       queryExpr,
					ResumeToken: remoteTokens[vid],
				})
				mu.Lock()
				streams = append(streams, vaultStream{records: recCh, errCh: eCh, getHistogram: getHist, vaultID: vid})
				mu.Unlock()
			})
		}
	}
	wg.Wait()

	for _, vs := range streams {
		if vs.getHistogram != nil {
			allHist = mergeHistogramBuckets(allHist, vs.getHistogram())
		}
	}

	if len(streams) == 0 {
		return nil, allHist, nil
	}

	// The vaults this search fanned out to — the contributor set under the
	// fail-on-remote-failure policy. Sorted for a deterministic wire order.
	contributingVaults := make([]glid.GLID, 0, len(streams))
	for _, vs := range streams {
		contributingVaults = append(contributingVaults, vs.vaultID)
	}
	slices.SortFunc(contributingVaults, func(a, b glid.GLID) int {
		return cmp.Compare(a.String(), b.String())
	})

	// Convert each channel into an iter.Seq2[chunk.Record, error].
	var iters []iter.Seq2[chunk.Record, error]
	for _, vs := range streams {
		iters = append(iters, channelToIter(vs.records, vs.errCh))
	}

	// If only one remote vault, return its iterator directly.
	if len(iters) == 1 {
		return iters[0], allHist, contributingVaults
	}

	// K-way merge of N iterators using a heap.
	merged := kWayMerge(iters, q.OrderBy, q.Reverse())
	return merged, allHist, contributingVaults
}

// remoteVaultsByNode groups remote vault IDs by their owning node.
// When selectedVaults is non-nil, only vaults in that set are included
// (used when the query contains a vault_id=X filter).
//
// Placements come from their owner via placementsFor.
func (s *QueryServer) remoteVaultsByNode(ctx context.Context, selectedVaults []glid.GLID) map[string][]glid.GLID {
	return s.remoteVaultsByNodeFiltered(ctx, selectedVaults, s.orch.LocalLeaderVaultIDs())
}

func (s *QueryServer) remoteVaultsByNodeFiltered(ctx context.Context, selectedVaults []glid.GLID, localVaultIDs map[glid.GLID]bool) map[string][]glid.GLID {
	vaults, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}

	selected := make(map[glid.GLID]bool, len(selectedVaults))
	for _, id := range selectedVaults {
		selected[id] = true
	}

	byNode := make(map[string][]glid.GLID)
	for _, v := range vaults {
		if len(selected) > 0 && !selected[v.ID] {
			continue
		}
		if localVaultIDs[v.ID] {
			continue // searched locally, skip remote
		}
		placements := s.placementsFor(ctx, v.ID)
		if len(placements) == 0 {
			continue
		}
		leaderNodeID := system.LeaderNodeID(placements, nscs)
		if leaderNodeID == "" || leaderNodeID == s.localNodeID {
			continue
		}
		byNode[leaderNodeID] = append(byNode[leaderNodeID], v.ID)
	}
	return byNode
}

// mergeHistogramBuckets sums two histogram bucket slices by matching timestamp.
// The result is sorted by timestamp to ensure chronological order even when
// remote nodes produce slightly different bucket boundaries (e.g. from
// independent "last=5m" resolution with clock skew).
//
// HasCloudData/CloudCount (see gastrolog-4of7c) merge like Count/GroupCounts,
// not like an ordinary group key: HasCloudData ORs across nodes (a bucket
// touched by cloud-derived data on ANY node stays flagged) and CloudCount
// sums (each node contributes its own local applyCloudSelectivity estimate
// for the bucket). Dropping these on merge would silently present a
// cluster-wide bucket as exact whenever the node whose entry happened to
// seed `a` for that timestamp had no cloud-backed chunks locally.
func mergeHistogramBuckets(a, b []*apiv1.HistogramBucket) []*apiv1.HistogramBucket {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	idx := make(map[int64]int, len(a))
	for i, bucket := range a {
		idx[bucket.TimestampMs] = i
	}
	for _, bucket := range b {
		if i, ok := idx[bucket.TimestampMs]; ok {
			a[i].Count += bucket.Count
			if bucket.HasCloudData {
				a[i].HasCloudData = true
			}
			a[i].CloudCount += bucket.CloudCount
			for k, v := range bucket.GroupCounts {
				if a[i].GroupCounts == nil {
					a[i].GroupCounts = make(map[string]int64)
				}
				a[i].GroupCounts[k] += v
			}
		} else {
			idx[bucket.TimestampMs] = len(a)
			a = append(a, bucket)
		}
	}
	slices.SortFunc(a, func(x, y *apiv1.HistogramBucket) int {
		return cmp.Compare(x.TimestampMs, y.TimestampMs)
	})
	return a
}

// HistogramToProto converts internal histogram buckets to the proto type.
func HistogramToProto(buckets []query.HistogramBucket) []*apiv1.HistogramBucket {
	if len(buckets) == 0 {
		return nil
	}
	out := make([]*apiv1.HistogramBucket, len(buckets))
	for i, b := range buckets {
		out[i] = &apiv1.HistogramBucket{
			TimestampMs:  b.TimestampMs,
			Count:        b.Count,
			GroupCounts:  b.GroupCounts,
			HasCloudData: b.HasCloudData,
			CloudCount:   b.CloudCount,
		}
	}
	return out
}

// channelToIter converts a channel of ExportRecord batches + error channel
// into an iter.Seq2[chunk.Record, error].
func channelToIter(recCh <-chan []*apiv1.ExportRecord, errCh <-chan error) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		for batch := range recCh {
			for _, er := range batch {
				rec := convert.ExportToRecord(er)
				if !yield(rec, nil) {
					return
				}
			}
		}
		// Check for stream error after records are drained.
		if err, ok := <-errCh; ok && err != nil {
			yield(chunk.Record{}, err)
		}
	}
}

// mergeEntry holds a record and the index of the source iterator it came from.
type mergeEntry struct {
	rec chunk.Record
	idx int
}

// mergeState holds the pull function and stop function for one iterator.
type mergeState struct {
	next func() (chunk.Record, error, bool)
	stop func()
}

// kWayMerge merges N sorted iterators into one sorted iterator.
// N is small (typically 1-3 remote nodes), so selection-based min-finding
// is used instead of a heap.
func kWayMerge(iters []iter.Seq2[chunk.Record, error], orderBy query.OrderBy, reverse bool) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		states, entries, err := initMerge(iters)
		if err != nil {
			yield(chunk.Record{}, err)
			stopAll(states)
			return
		}
		defer stopAll(states)

		less := buildMergeLess(orderBy, reverse)
		runMerge(yield, states, entries, less)
	}
}

// initMerge starts all iterators and pulls the first record from each.
func initMerge(iters []iter.Seq2[chunk.Record, error]) ([]mergeState, []mergeEntry, error) {
	states := make([]mergeState, len(iters))
	var entries []mergeEntry
	for i, it := range iters {
		next, stop := iter.Pull2(it)
		states[i] = mergeState{next: next, stop: stop}
		rec, err, ok := next()
		if !ok {
			stop()
			states[i].stop = nil
			continue
		}
		if err != nil {
			return states, nil, err
		}
		entries = append(entries, mergeEntry{rec: rec, idx: i})
	}
	return states, entries, nil
}

// stopAll stops all active iterators.
func stopAll(states []mergeState) {
	for i := range states {
		if states[i].stop != nil {
			states[i].stop()
		}
	}
}

// buildMergeLess returns a comparison function for merge entries.
func buildMergeLess(orderBy query.OrderBy, reverse bool) func(a, b mergeEntry) bool {
	return func(a, b mergeEntry) bool {
		ta := orderBy.RecordTS(a.rec)
		tb := orderBy.RecordTS(b.rec)
		if reverse {
			return ta.After(tb)
		}
		return ta.Before(tb)
	}
}

// runMerge performs the k-way merge loop.
func runMerge(yield func(chunk.Record, error) bool, states []mergeState, entries []mergeEntry, less func(a, b mergeEntry) bool) {
	for len(entries) > 0 {
		minIdx := 0
		for i := 1; i < len(entries); i++ {
			if less(entries[i], entries[minIdx]) {
				minIdx = i
			}
		}

		rec := entries[minIdx].rec
		srcIdx := entries[minIdx].idx

		nextRec, err, ok := states[srcIdx].next()
		if err != nil {
			yield(chunk.Record{}, err)
			return
		}
		if ok {
			entries[minIdx].rec = nextRec
		} else {
			entries[minIdx] = entries[len(entries)-1]
			entries = entries[:len(entries)-1]
		}

		if !yield(rec, nil) {
			return
		}
	}
}

// collectRemotePipeline fans out a pipeline query to all remote vaults and
// collects their TableResults. Each remote node runs the full pipeline locally
// (the executor detects the pipeline and calls RunPipeline). The coordinating
// node then merges the results.
//
// The expression is reconstructed from the parsed q and pipeline with absolute
// start/end timestamps so all nodes use identical time windows (avoids bucket
// misalignment from re-evaluating relative "last=5m" on each node).
//
// Fail-on-remote-failure (gastrolog-20lrg): if ANY remote vault fails, the
// whole pipeline query fails — a partial aggregate is silently wrong. A
// `| stats count` or `| timechart` collapses many vaults into one number or
// table; dropping a failed vault (the previous behaviour) undercounts with no
// signal to the caller, presenting a derived-from-a-subset figure as
// authoritative — exactly the data-integrity failure the project forbids, and
// worse than an inspector fan-out where a missing node is a visible per-row
// gap. There is no per-vault affordance in a merged stats table for a
// "partial" badge, so returning-partial-with-report is not an option here.
// This matches the two sibling query paths, which both already abort on any
// remote error: searchDirect (mergeIterators → mapSearchError) and
// searchPipelineGlobal (collectRemote consumption). The fan-out runs under the
// request ctx — the same latency budget (s.queryTimeout) search uses — not the
// inspector per-peer timeout, so a slow-but-alive vault is not failed faster
// than the equivalent raw search would be.
func (s *QueryServer) collectRemotePipeline(ctx context.Context, q query.Query, pipeline *querylang.Pipeline) ([]*query.TableResult, error) {
	if s.remoteSearcher == nil || s.cfgStore == nil {
		return nil, nil
	}
	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	byNode := s.remoteVaultsByNode(ctx, selectedVaults)
	if len(byNode) == 0 {
		return nil, nil
	}

	// Reconstruct expression with absolute timestamps so remote nodes
	// produce identical timechart bucket boundaries.
	// Pipeline.String() uses " | " between parts but omits a leading "|"
	// when there is no filter. Prefix with "| " to ensure the remote parser
	// sees the pipe operator.
	pipelineStr := pipeline.String()
	if len(pipelineStr) > 0 && pipelineStr[0] != '|' {
		pipelineStr = "| " + pipelineStr
	}
	remoteExpr := q.String() + " " + pipelineStr

	// Fan out RPCs concurrently — one goroutine per remote vault.
	type pipelineFetch struct {
		nodeID string
		vid    glid.GLID
	}
	var fetches []pipelineFetch
	for nodeID, vaultIDs := range byNode {
		for _, vid := range vaultIDs {
			fetches = append(fetches, pipelineFetch{nodeID, vid})
		}
	}
	responses := make([]*apiv1.ForwardSearchResponse, len(fetches))
	fetchErrors := make([]error, len(fetches))
	var wg sync.WaitGroup
	for i, f := range fetches {
		wg.Go(func() {
			// Runs under the request ctx (bounded by s.queryTimeout, the
			// same budget the streaming search uses) — not a per-peer
			// inspector timeout: a pipeline is a query, so it must share
			// search's latency policy, and wg.Wait() is bounded by the
			// query timeout just as search's remote merge is.
			responses[i], fetchErrors[i] = s.remoteSearcher.Search(ctx, f.nodeID, &apiv1.ForwardSearchRequest{
				VaultId: f.vid.ToProto(),
				Query:   remoteExpr,
			})
		})
	}
	wg.Wait()

	var results []*query.TableResult
	for i, resp := range responses {
		if fetchErrors[i] != nil {
			// Fail hard — a partial aggregate is a wrong number. Abort the
			// whole pipeline query with the remote error, mapped the same
			// way the streaming search maps a propagated remote error.
			return nil, mapSearchError(fmt.Errorf("pipeline: remote vault %s on node %s failed: %w",
				fetches[i].vid, fetches[i].nodeID, fetchErrors[i]))
		}
		if resp.GetTableResult() != nil {
			if tr := protoToTableResult(resp.GetTableResult()); tr != nil {
				results = append(results, tr)
			}
		}
	}

	if len(results) > 0 {
		s.logger.Debug("pipeline: collected remote table results", "nodes", len(byNode), "tables", len(results))
	}
	return results, nil
}
