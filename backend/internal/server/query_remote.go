package server

import (
	"cmp"
	"context"
	"iter"
	"maps"
	"slices"
	"sync"

	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// collectRemote opens streaming ForwardSearch RPCs to all remote vaults and
// returns a merged sorted iterator over their records plus the combined
// histogram. The iterator performs a k-way merge — at most one record per
// remote vault is held in memory at any time.
func (s *QueryServer) collectRemote(ctx context.Context, q query.Query, remoteTokens map[uuid.UUID][]byte) (iter.Seq2[chunk.Record, error], []*apiv1.HistogramBucket, func() map[uuid.UUID][]byte) {
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
		remoteTokens = make(map[uuid.UUID][]byte)
	}

	// Fan out streaming RPCs concurrently — one per remote vault.
	type vaultStream struct {
		records        <-chan []*apiv1.ExportRecord
		errCh          <-chan error
		getResumeToken func() []byte
		vaultID        uuid.UUID
	}
	var streams []vaultStream
	var allHist []*apiv1.HistogramBucket
	var mu sync.Mutex
	var wg sync.WaitGroup

	for nodeID, vaultIDs := range byNode {
		for _, vid := range vaultIDs {
			wg.Go(func() {
				recCh, hist, _, eCh, getToken := s.remoteSearcher.SearchStream(ctx, nodeID, &apiv1.ForwardSearchRequest{
					VaultId:     vid.String(),
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

	getRemoteTokens := func() map[uuid.UUID][]byte {
		tokens := make(map[uuid.UUID][]byte)
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

// remoteVaultsByNode groups remote vault IDs by their owning node.
// When selectedVaults is non-nil, only vaults in that set are included
// (used when the query contains a vault_id=X filter).
//
// Uses tier-level NodeID (set by the placement manager) for node assignment.
func (s *QueryServer) remoteVaultsByNode(ctx context.Context, selectedVaults []uuid.UUID) map[string][]uuid.UUID {
	vaults, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil
	}
	tiers, err := s.cfgStore.ListTiers(ctx)
	if err != nil {
		return nil
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}

	selected := make(map[uuid.UUID]bool, len(selectedVaults))
	for _, id := range selectedVaults {
		selected[id] = true
	}

	// For each vault, fan out to remote primaries for tiers this node
	// doesn't have locally. Tiers that exist locally (as primary or
	// secondary) are searched by the local query engine — don't
	// double-query their primary remotely.
	localTierIDs := s.orch.LocalPrimaryTierIDs()

	tierMap := make(map[uuid.UUID]*config.TierConfig, len(tiers))
	for i := range tiers {
		tierMap[tiers[i].ID] = &tiers[i]
	}

	byNode := make(map[string][]uuid.UUID)
	for _, v := range vaults {
		if len(selected) > 0 && !selected[v.ID] {
			continue
		}
		seen := make(map[string]bool)
		for _, tierID := range config.VaultTierIDs(tiers, v.ID) {
			if localTierIDs[tierID] {
				continue // searched locally, skip remote
			}
			tc := tierMap[tierID]
			if tc == nil {
				continue
			}
			leaderNodeID := tc.LeaderNodeID(nscs)
			if leaderNodeID == "" || leaderNodeID == s.localNodeID {
				continue
			}
			if !seen[leaderNodeID] {
				seen[leaderNodeID] = true
				byNode[leaderNodeID] = append(byNode[leaderNodeID], v.ID)
			}
		}
	}
	return byNode
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

// histogramToProto converts internal histogram buckets to the proto type.
func histogramToProto(buckets []query.HistogramBucket) []*apiv1.HistogramBucket {
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
				rec := exportRecordToChunkRecord(er)
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

// exportRecordToChunkRecord converts a proto ExportRecord to a chunk.Record.
func exportRecordToChunkRecord(er *apiv1.ExportRecord) chunk.Record {
	rec := chunk.Record{Raw: er.GetRaw()}
	if er.GetSourceTs() != nil {
		rec.SourceTS = er.GetSourceTs().AsTime()
	}
	if er.GetIngestTs() != nil {
		rec.IngestTS = er.GetIngestTs().AsTime()
	}
	if er.GetWriteTs() != nil {
		rec.WriteTS = er.GetWriteTs().AsTime()
	}
	if len(er.GetAttrs()) > 0 {
		rec.Attrs = make(chunk.Attributes, len(er.GetAttrs()))
		maps.Copy(rec.Attrs, er.GetAttrs())
	}
	if er.GetVaultId() != "" {
		rec.VaultID, _ = uuid.Parse(er.GetVaultId())
	}
	if er.GetChunkId() != "" {
		rec.Ref.ChunkID, _ = chunk.ParseChunkID(er.GetChunkId())
		rec.Ref.Pos = er.GetPos()
	}
	rec.EventID.IngestSeq = er.GetIngestSeq()
	if len(er.GetIngesterId()) == 16 {
		copy(rec.EventID.IngesterID[:], er.GetIngesterId())
	}
	rec.EventID.IngestTS = rec.IngestTS
	return rec
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
		vid    uuid.UUID
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
			responses[i], fetchErrors[i] = s.remoteSearcher.Search(ctx, f.nodeID, &apiv1.ForwardSearchRequest{
				VaultId: f.vid.String(),
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
