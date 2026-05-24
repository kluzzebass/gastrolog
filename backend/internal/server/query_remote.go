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
	"gastrolog/internal/convert"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"
)

// collectRemote opens streaming ForwardSearch RPCs to all remote vaults and
// returns a merged sorted iterator over their records plus the combined
// histogram. The iterator performs a k-way merge — at most one record per
// remote vault is held in memory at any time.
func (s *QueryServer) collectRemote(ctx context.Context, q query.Query, remoteTokens map[glid.GLID][]byte) (iter.Seq2[chunk.Record, error], []*apiv1.HistogramBucket, func() map[glid.GLID][]byte) {
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
		records        <-chan []*apiv1.ExportRecord
		errCh          <-chan error
		getResumeToken func() []byte
		vaultID        glid.GLID
	}
	var streams []vaultStream
	var allHist []*apiv1.HistogramBucket
	var mu sync.Mutex
	var wg sync.WaitGroup

	for nodeID, vaultIDs := range byNode {
		for _, vid := range vaultIDs {
			wg.Go(func() {
				recCh, hist, _, eCh, getToken := s.remoteSearcher.SearchStream(ctx, nodeID, &apiv1.ForwardSearchRequest{
					VaultId:     vid.ToProto(),
					Query:       queryExpr,
					ResumeToken: remoteTokens[vid],
				})
				mu.Lock()
				streams = append(streams, vaultStream{records: recCh, errCh: eCh, getResumeToken: getToken, vaultID: vid})
				allHist = mergeHistogramBuckets(allHist, hist)
				mu.Unlock()
			})
		}
	}
	wg.Wait()

	getRemoteTokens := func() map[glid.GLID][]byte {
		tokens := make(map[glid.GLID][]byte)
		for _, vs := range streams {
			if vs.getResumeToken != nil {
				if t := vs.getResumeToken(); len(t) > 0 {
					tokens[vs.vaultID] = t
				}
			}
		}
		return tokens
	}

	if len(streams) == 0 {
		return nil, allHist, nil
	}

	// Convert each channel into an iter.Seq2[chunk.Record, error].
	var iters []iter.Seq2[chunk.Record, error]
	for _, vs := range streams {
		iters = append(iters, channelToIter(vs.records, vs.errCh))
	}

	// If only one remote vault, return its iterator directly.
	if len(iters) == 1 {
		return iters[0], allHist, getRemoteTokens
	}

	// K-way merge of N iterators using a heap.
	merged := kWayMerge(iters, q.OrderBy, q.Reverse())
	return merged, allHist, getRemoteTokens
}

// remoteVaultsByNode groups remote vault IDs by their owning nodes.
// When selectedVaults is non-nil, only vaults in that set are included
// (used when the query contains a vault_id=X filter).
//
// Under fan-out the same vault appears under every placement member that
// isn't this node — each member is a peer Receiver and may hold records
// the others don't. The k-way merge + dedupWindow on the coordinator
// collapses cross-replica duplicates by EventID.
//
// Local-vault note: when this node is itself a placement member, we
// STILL enumerate peers. Pre-fan-out the local engine held canonical
// data so peers were redundant; under fan-out every Receiver's active
// chunk diverges until catchup converges them (gastrolog-hshgl), so
// peers can hold records this node hasn't seen yet. The local engine
// runs in parallel via the search path; dedupWindow folds the streams.
//
// Reads VaultConfig.Placements directly (mirrored from vault placements
// via the FSM bridge — gastrolog-257l7).
func (s *QueryServer) remoteVaultsByNode(ctx context.Context, selectedVaults []glid.GLID) map[string][]glid.GLID {
	return s.remoteVaultsByNodeFiltered(ctx, selectedVaults)
}

func (s *QueryServer) remoteVaultsByNodeFiltered(ctx context.Context, selectedVaults []glid.GLID) map[string][]glid.GLID {
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
		if len(v.Placements) == 0 {
			continue
		}
		// Fan-out reads: route to every Receiving member of the vault's
		// active chunk, not just the leader. Each member may hold records
		// the others don't (post-senderChunkID-strip), and the existing
		// dedupWindow at the merge boundary collapses cross-replica
		// duplicates by EventID. Bandwidth cost is N× per query —
		// acceptable trade for correctness under fan-out divergence.
		//
		// Self is excluded by the nodeID == s.localNodeID check: the
		// local engine path handles this node's data directly. Local
		// vaults are NOT skipped at the vault level (see godoc above).
		//
		// Prefer the active chunk's Receiving set from the FSM
		// (gastrolog-6bt8s): under fan-out, Receiving is the
		// authoritative "where records currently land" set, and a node
		// can be in placement without being in Receiving (e.g. mid-drain
		// once gastrolog-68cfq wires CmdRemoveReceiving). Falls back to
		// placement-derived nodes when there's no active chunk yet
		// (first record into a new vault) or when this node has no
		// FSM access (single-node / memory mode).
		nodes := s.activeReceivingNodes(v.ID)
		if len(nodes) == 0 {
			nodes = system.PlacementNodeIDs(v.Placements, nscs)
		}
		for _, nodeID := range nodes {
			if nodeID == "" || nodeID == s.localNodeID {
				continue
			}
			byNode[nodeID] = append(byNode[nodeID], v.ID)
		}
	}
	return byNode
}

// activeReceivingNodes wraps the orchestrator's ActiveReceivingNodes for
// safe call sites where the orchestrator may not be wired (tests,
// single-node bootstrap). Returns nil when unavailable so the caller
// falls back to placement-derived nodes.
func (s *QueryServer) activeReceivingNodes(vaultID glid.GLID) []string {
	if s.orch == nil {
		return nil
	}
	return s.orch.ActiveReceivingNodes(vaultID)
}

// mergeHistogramBuckets sums two histogram bucket slices by matching timestamp.
// The result is sorted by timestamp to ensure chronological order even when
// remote nodes produce slightly different bucket boundaries (e.g. from
// independent "last=5m" resolution with clock skew).
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

// normalizeHistogramGroupCounts forces every bucket's per-level GroupCounts to
// sum to that bucket's authoritative Count. The Count is rank-arithmetic over
// the deduplicated chunk set (and matches `| stats count`); the level
// breakdown is a sampled estimate produced by a parallel path that does NOT
// share the record stream's cross-vault/cross-replica dedup, so summing
// per-source breakdowns at the merge boundary can overshoot Count (the
// inspector "spike" bug — the volume bar is a stack of GroupCounts, so an
// overshoot renders as a too-tall bar). Re-projecting the breakdown onto the
// authoritative Count makes the bar height honest while preserving the
// sampled level *proportions*. Applied once, on the coordinator, after the
// cross-node + cross-vault merge — the single point where Count is final.
//
// Buckets where the breakdown already sums to Count are untouched (the common
// case). A bucket with Count > 0 but no breakdown at all is left alone: the
// caller's downstream "other" handling owns that case.
func normalizeHistogramGroupCounts(buckets []*apiv1.HistogramBucket) {
	for _, b := range buckets {
		if b == nil || b.Count <= 0 || len(b.GroupCounts) == 0 {
			continue
		}
		var sum int64
		for _, v := range b.GroupCounts {
			sum += v
		}
		if sum == b.Count || sum <= 0 {
			continue
		}
		// Scale each level proportionally to the authoritative Count.
		// Integer division drops fractional remainder; assign the leftover
		// to the largest level so the rendered segments sum to exactly Count
		// (no thin sliver bar, no overshoot).
		scaled := make(map[string]int64, len(b.GroupCounts))
		var assigned int64
		var largestKey string
		var largestVal int64
		for k, v := range b.GroupCounts {
			s := v * b.Count / sum
			scaled[k] = s
			assigned += s
			if v > largestVal {
				largestVal = v
				largestKey = k
			}
		}
		if rem := b.Count - assigned; rem != 0 && largestKey != "" {
			scaled[largestKey] += rem
		}
		b.GroupCounts = scaled
	}
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
func (s *QueryServer) collectRemotePipeline(ctx context.Context, q query.Query, pipeline *querylang.Pipeline) []*query.TableResult {
	if s.remoteSearcher == nil || s.cfgStore == nil {
		return nil
	}
	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	byNode := s.remoteVaultsByNode(ctx, selectedVaults)
	if len(byNode) == 0 {
		return nil
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
			// Per-peer timeout (gastrolog-csspr): a paused peer with the
			// parent ctx alone would hang this goroutine indefinitely,
			// and wg.Wait() would block the whole pipeline handler.
			// Bounding each call independently means total fan-out is
			// max(peer RTTs) capped by peerInspectorTimeout.
			peerCtx, cancel := context.WithTimeout(ctx, peerInspectorTimeout)
			defer cancel()
			responses[i], fetchErrors[i] = s.remoteSearcher.Search(peerCtx, f.nodeID, &apiv1.ForwardSearchRequest{
				VaultId: f.vid.ToProto(),
				Query:   remoteExpr,
			})
		})
	}
	wg.Wait()

	var results []*query.TableResult
	for i, resp := range responses {
		if fetchErrors[i] != nil {
			s.logger.Warn("pipeline: remote vault failed", "node", fetches[i].nodeID, "vault", fetches[i].vid, "err", fetchErrors[i])
			continue
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
	return results
}
