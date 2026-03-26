package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/lookup"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// RemoteSearcher sends search and context requests to remote cluster nodes.
// Nil in single-node mode.
type RemoteSearcher interface {
	// Search collects a full streamed ForwardSearch response. Used by
	// collectRemotePipeline which needs the complete TableResult.
	Search(ctx context.Context, nodeID string, req *apiv1.ForwardSearchRequest) (*apiv1.ForwardSearchResponse, error)
	// SearchStream opens a streaming ForwardSearch.
	// Returns record batches channel, histogram, tableResult, error channel,
	// and a function to retrieve the resume token after draining records.
	SearchStream(ctx context.Context, nodeID string, req *apiv1.ForwardSearchRequest) (
		records <-chan []*apiv1.ExportRecord,
		histogram []*apiv1.HistogramBucket,
		tableResult *apiv1.TableResult,
		errCh <-chan error,
		getResumeToken func() []byte,
	)
	GetContext(ctx context.Context, nodeID string, req *apiv1.ForwardGetContextRequest) (*apiv1.ForwardGetContextResponse, error)
	Explain(ctx context.Context, nodeID string, req *apiv1.ForwardExplainRequest) (*apiv1.ForwardExplainResponse, error)
	Follow(ctx context.Context, nodeID string, req *apiv1.ForwardFollowRequest) (<-chan *apiv1.ExportRecord, <-chan error)
	ExportToVault(ctx context.Context, nodeID string, req *apiv1.ForwardExportToVaultRequest) (*apiv1.ForwardExportToVaultResponse, error)
}

// QueryServer implements the QueryService.
type QueryServer struct {
	orch              *orchestrator.Orchestrator
	cfgStore          config.Store
	remoteSearcher    RemoteSearcher
	localNodeID       string
	lookupResolver    lookup.Resolver
	lookupNames       []string
	queryTimeout      time.Duration
	maxFollowDuration time.Duration // 0 = no limit
	maxResultCount    int64         // 0 = unlimited
	logger            *slog.Logger
}

var _ gastrologv1connect.QueryServiceHandler = (*QueryServer)(nil)

// NewQueryServer creates a new QueryServer.
func NewQueryServer(orch *orchestrator.Orchestrator, cfgStore config.Store, remoteSearcher RemoteSearcher, localNodeID string, lookupResolver lookup.Resolver, lookupNames []string, queryTimeout, maxFollowDuration time.Duration, maxResultCount int64, logger *slog.Logger) *QueryServer {
	return &QueryServer{orch: orch, cfgStore: cfgStore, remoteSearcher: remoteSearcher, localNodeID: localNodeID, lookupResolver: lookupResolver, lookupNames: lookupNames, queryTimeout: queryTimeout, maxFollowDuration: maxFollowDuration, maxResultCount: maxResultCount, logger: logger}
}

// Search executes a query and streams matching records.
// Searches across all vaults; use vault_id=X in query expression to filter.
func (s *QueryServer) Search(
	ctx context.Context,
	req *connect.Request[apiv1.SearchRequest],
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}

	eng := s.orch.MultiVaultQueryEngine()
	if s.lookupResolver != nil {
		eng.SetLookupResolver(s.lookupResolver)
	}

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	if pipeline != nil && len(pipeline.Pipes) > 0 {
		// Reject queries with export operator — must route through ExportToVault RPC.
		if _, hasExport := querylang.HasExportOp(pipeline); hasExport {
			return connect.NewError(connect.CodeInvalidArgument,
				errors.New("queries with | export must use the ExportToVault RPC"))
		}

		if query.CanStreamPipeline(pipeline) {
			// Streamable pipeline: apply ops per-record on top of the
			// normal search iterator with full resume-token support.
			transform := query.NewRecordTransform(pipeline.Pipes, s.lookupResolver)
			return s.searchDirect(ctx, eng, q, req.Msg.ResumeToken, transform, stream)
		}
		// Aggregating / full-materialization pipeline (stats, timechart,
		// sort, tail, slice, raw).
		return s.searchPipeline(ctx, eng, q, pipeline, stream)
	}

	return s.searchDirect(ctx, eng, q, req.Msg.ResumeToken, nil, stream)
}

// searchPipeline handles pipelines that require full materialization
// (stats, timechart, sort, tail, slice, raw).
func (s *QueryServer) searchPipeline(
	ctx context.Context,
	eng *query.Engine,
	q query.Query,
	pipeline *querylang.Pipeline,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	// Non-distributive cap (head/tail/slice) before aggregation: gather raw
	// records from all nodes, then run the pipeline on the coordinator.
	if query.PipelineNeedsGlobalRecords(pipeline) {
		return s.searchPipelineGlobal(ctx, eng, q, pipeline, stream)
	}

	if s.maxResultCount > 0 && (q.Limit == 0 || int64(q.Limit) > s.maxResultCount) {
		q.Limit = int(s.maxResultCount)
	}
	result, err := eng.RunPipeline(ctx, q, pipeline)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	// Compute local histogram to include alongside pipeline results.
	histogram := histogramToProto(eng.ComputeHistogram(ctx, q, 50))

	if result.Table != nil {
		// Fan out to remote nodes and merge table results.
		remoteResults := s.collectRemotePipeline(ctx, q, pipeline)
		if len(remoteResults) > 0 {
			result.Table = mergeTableResults(result.Table, remoteResults)
		}
		return stream.Send(&apiv1.SearchResponse{
			TableResult: tableResultToProto(result.Table, pipeline),
			Histogram:   histogram,
		})
	}
	// Non-aggregating but needs full materialization (sort/tail/slice):
	// stream all records.
	batch := make([]*apiv1.Record, 0, 100)
	for _, rec := range result.Records {
		batch = append(batch, recordToProto(rec))
		if len(batch) >= 100 {
			if err := stream.Send(&apiv1.SearchResponse{Records: batch, HasMore: true}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	return stream.Send(&apiv1.SearchResponse{Records: batch, Histogram: histogram})
}

// searchPipelineGlobal handles pipelines where non-distributive cap operators
// (head, tail, slice) precede an aggregation (stats/timechart). Instead of
// fanning out the full pipeline to each remote node (which would apply the cap
// independently per-node), it gathers raw records from all remote nodes, then
// runs the entire pipeline on the coordinator.
func (s *QueryServer) searchPipelineGlobal(
	ctx context.Context,
	eng *query.Engine,
	q query.Query,
	pipeline *querylang.Pipeline,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	if s.maxResultCount > 0 && (q.Limit == 0 || int64(q.Limit) > s.maxResultCount) {
		q.Limit = int(s.maxResultCount)
	}

	// Collect raw records from remote nodes (no pipeline — just the base query).
	remoteIter, remoteHist, _ := s.collectRemote(ctx, q, nil)
	var extraRecords []chunk.Record
	if remoteIter != nil {
		for rec, iterErr := range remoteIter {
			if iterErr != nil {
				return connect.NewError(connect.CodeInternal, iterErr)
			}
			extraRecords = append(extraRecords, rec)
		}
	}

	result, err := eng.RunPipelineOnRecords(ctx, q, pipeline, extraRecords)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	// Compute and merge histogram.
	localHist := histogramToProto(eng.ComputeHistogram(ctx, q, 50))
	histogram := mergeHistogramBuckets(localHist, remoteHist)

	if result.Table != nil {
		return stream.Send(&apiv1.SearchResponse{
			TableResult: tableResultToProto(result.Table, pipeline),
			Histogram:   histogram,
		})
	}

	batch := make([]*apiv1.Record, 0, 100)
	for _, rec := range result.Records {
		batch = append(batch, recordToProto(rec))
		if len(batch) >= 100 {
			if err := stream.Send(&apiv1.SearchResponse{Records: batch, HasMore: true}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	return stream.Send(&apiv1.SearchResponse{Records: batch, Histogram: histogram})
}

// searchDirect streams search results, merging local and remote vault results
// in timestamp order. When transform is non-nil, per-record pipeline transforms
// are applied. Remote results stream end-to-end — no full-result-set buffering.
func (s *QueryServer) searchDirect(
	ctx context.Context,
	eng *query.Engine,
	q query.Query,
	resumeTokenData []byte,
	transform *query.RecordTransform,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	if s.maxResultCount > 0 && (q.Limit == 0 || int64(q.Limit) > s.maxResultCount) {
		q.Limit = int(s.maxResultCount)
	}

	var resume *query.ResumeToken
	if len(resumeTokenData) > 0 {
		// Resume tokens with non-default ordering are not yet supported.
		if q.OrderBy != query.OrderByIngestTS {
			return connect.NewError(connect.CodeUnimplemented,
				fmt.Errorf("pagination with order=%s is not yet supported", q.OrderBy))
		}
		var err error
		resume, err = ProtoToResumeToken(resumeTokenData)
		if err != nil {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
		// Restore frozen time bounds from page 1 so "last-5m" doesn't shift.
		if !resume.FrozenStart.IsZero() {
			q.Start = resume.FrozenStart
		}
		if !resume.FrozenEnd.IsZero() {
			q.End = resume.FrozenEnd
		}
	}

	// The frozen bounds are now in q.Start/q.End (either from the original
	// query or restored from the resume token above).
	frozenStart, frozenEnd := q.Start, q.End

	localResume, remoteTokens := s.splitResumeToken(resume)

	// Collect remote results as a streaming iterator.
	remoteIter, remoteHist, getRemoteTokens := s.collectRemote(ctx, q, remoteTokens)

	// Compute local histogram and merge with remote.
	localHist := histogramToProto(eng.ComputeHistogram(ctx, q, 50))
	histogram := mergeHistogramBuckets(localHist, remoteHist)

	localIter, getLocalToken := eng.Search(ctx, q, localResume)

	// Combine local + remote resume tokens into a unified vault token map.
	getToken := func() *query.ResumeToken {
		token := getLocalToken()
		if token == nil {
			token = &query.ResumeToken{}
		}
		if getRemoteTokens != nil {
			if token.VaultTokens == nil {
				token.VaultTokens = make(map[uuid.UUID][]byte)
			}
			maps.Copy(token.VaultTokens, getRemoteTokens())
		}
		hasPositions := len(token.Positions) > 0
		hasVaultTokens := len(token.VaultTokens) > 0
		if !hasPositions && !hasVaultTokens {
			return nil
		}
		token.FrozenStart = frozenStart
		token.FrozenEnd = frozenEnd
		return token
	}

	return s.mergeAndStream(ctx, localIter, getToken, remoteIter, q.OrderBy, q.Reverse(), transform, histogram, stream)
}

// splitResumeToken separates a unified resume token into local positions
// (for eng.Search) and remote opaque blobs (for collectRemote).
func (s *QueryServer) splitResumeToken(resume *query.ResumeToken) (*query.ResumeToken, map[uuid.UUID][]byte) {
	if resume == nil || len(resume.VaultTokens) == 0 {
		return nil, nil
	}

	// No local-vault skip — a vault may have some tiers local and others
	// remote. Both need to be searched. The ForwardSearch handler on the
	// remote node only searches its LOCAL tiers, so no double-counting.

	remoteTokens := make(map[uuid.UUID][]byte)
	var localPositions []query.MultiVaultPosition
	for vid, tokenData := range resume.VaultTokens {
		if s.orch.HasLocalQueryEngine(vid) {
			positions, err := VaultTokenToPositions(tokenData)
			if err != nil {
				continue
			}
			localPositions = append(localPositions, positions...)
		} else {
			remoteTokens[vid] = tokenData
		}
	}

	var localResume *query.ResumeToken
	if len(localPositions) > 0 {
		localResume = &query.ResumeToken{Positions: localPositions}
	}
	return localResume, remoteTokens
}

func mapSearchError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, query.ErrInvalidResumeToken):
		return connect.NewError(connect.CodeInvalidArgument, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
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

	selected := make(map[uuid.UUID]bool, len(selectedVaults))
	for _, id := range selectedVaults {
		selected[id] = true
	}

	// No local-vault skip — a vault may have some tiers local and others
	// remote. Both need to be searched. The ForwardSearch handler on the
	// remote node only searches its LOCAL tiers, so no double-counting.

	tierMap := make(map[uuid.UUID]*config.TierConfig, len(tiers))
	for i := range tiers {
		tierMap[tiers[i].ID] = &tiers[i]
	}

	byNode := make(map[string][]uuid.UUID)
	for _, v := range vaults {
		if len(selected) > 0 && !selected[v.ID] {
			continue
		}
		// Find ALL remote nodes that own tiers for this vault.
		// A vault may span multiple nodes; each must be queried.
		seen := make(map[string]bool)
		for _, tierID := range v.TierIDs {
			tc := tierMap[tierID]
			if tc == nil || tc.NodeID == "" || tc.NodeID == s.localNodeID {
				continue
			}
			if !seen[tc.NodeID] {
				seen[tc.NodeID] = true
				byNode[tc.NodeID] = append(byNode[tc.NodeID], v.ID)
			}
		}
	}
	return byNode
}

func exportToRecord(er *apiv1.ExportRecord) *apiv1.Record {
	rec := &apiv1.Record{
		Raw:        er.Raw,
		SourceTs:   er.SourceTs,
		IngestTs:   er.IngestTs,
		WriteTs:    er.WriteTs,
		IngestSeq:  er.IngestSeq,
		IngesterId: er.IngesterId,
	}
	if len(er.Attrs) > 0 {
		rec.Attrs = make(map[string]string, len(er.Attrs))
		maps.Copy(rec.Attrs, er.Attrs)
	}
	if er.VaultId != "" {
		rec.Ref = &apiv1.RecordRef{
			VaultId: er.VaultId,
			ChunkId: er.ChunkId,
			Pos:     er.Pos,
		}
	}
	return rec
}


// mergeAndStream interleaves the local engine iterator with a remote iterator
// in timestamp order, applies optional per-record transforms, and streams
// batches to the client. When remoteIter is nil (single-node), the merge is
// a no-op passthrough with zero overhead.
func (s *QueryServer) mergeAndStream(
	ctx context.Context,
	localIter iter.Seq2[chunk.Record, error],
	getToken func() *query.ResumeToken,
	remoteIter iter.Seq2[chunk.Record, error],
	orderBy query.OrderBy,
	reverse bool,
	transform *query.RecordTransform,
	histogram []*apiv1.HistogramBucket,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	sb := newStreamBatcher(stream, 100)

	if remoteIter != nil {
		// Two-way sorted merge of local and remote iterators.
		if err := mergeIterators(ctx, sb, localIter, remoteIter, orderBy, reverse, transform); err != nil {
			return err
		}
	} else {
		// Fast path: no remote results, just stream local.
		if err := streamLocal(ctx, sb, localIter, transform); err != nil {
			return err
		}
	}

	// Build resume token from local state only (remote is fully streamed).
	var tokenBytes []byte
	if transform == nil || !transform.Done() {
		token := getToken()
		if token != nil {
			tokenBytes = ResumeTokenToProto(token)
		}
	}
	return stream.Send(&apiv1.SearchResponse{
		Records:     sb.pending(),
		ResumeToken: tokenBytes,
		HasMore:     len(tokenBytes) > 0,
		Histogram:   histogram,
	})
}

// streamLocal streams local iterator results through the batcher.
func streamLocal(ctx context.Context, sb *streamBatcher, localIter iter.Seq2[chunk.Record, error], transform *query.RecordTransform) error {
	for rec, err := range localIter {
		if err != nil {
			return mapSearchError(err)
		}
		done, emitErr := emitRecord(ctx, sb, rec, transform)
		if emitErr != nil {
			return emitErr
		}
		if done {
			return nil
		}
	}
	return nil
}

// mergeIterators performs a two-way sorted merge of local and remote iterators,
// emitting records through the stream batcher in timestamp order.
func mergeIterators(
	ctx context.Context,
	sb *streamBatcher,
	localIter, remoteIter iter.Seq2[chunk.Record, error],
	orderBy query.OrderBy,
	reverse bool,
	transform *query.RecordTransform,
) error {
	isBefore := func(a, b time.Time) bool {
		if reverse {
			return a.After(b)
		}
		return a.Before(b)
	}

	// Pull one record ahead from each iterator using channels.
	type recOrErr struct {
		rec chunk.Record
		err error
		ok  bool
	}
	localCh := make(chan recOrErr, 1)
	remoteCh := make(chan recOrErr, 1)

	// Pump iterators into channels in goroutines.
	go func() {
		defer close(localCh)
		for rec, err := range localIter {
			localCh <- recOrErr{rec, err, true}
		}
	}()
	go func() {
		defer close(remoteCh)
		for rec, err := range remoteIter {
			remoteCh <- recOrErr{rec, err, true}
		}
	}()

	var localPending, remotePending *recOrErr

	pull := func(ch <-chan recOrErr) *recOrErr {
		v, ok := <-ch
		if !ok {
			return nil
		}
		return &recOrErr{v.rec, v.err, ok}
	}

	localPending = pull(localCh)
	remotePending = pull(remoteCh)

	for localPending != nil || remotePending != nil {
		var rec chunk.Record
		if localPending != nil && localPending.err != nil {
			return mapSearchError(localPending.err)
		}
		if remotePending != nil && remotePending.err != nil {
			return mapSearchError(remotePending.err)
		}

		switch {
		case localPending == nil:
			rec = remotePending.rec
			remotePending = pull(remoteCh)
		case remotePending == nil:
			rec = localPending.rec
			localPending = pull(localCh)
		default:
			localTS := orderBy.RecordTS(localPending.rec)
			remoteTS := orderBy.RecordTS(remotePending.rec)
			if isBefore(localTS, remoteTS) {
				rec = localPending.rec
				localPending = pull(localCh)
			} else {
				rec = remotePending.rec
				remotePending = pull(remoteCh)
			}
		}

		done, err := emitRecord(ctx, sb, rec, transform)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return nil
}

// emitRecord applies an optional transform to a record and writes it to the
// batcher. Returns (done, err) where done=true means the transform is exhausted.
func emitRecord(ctx context.Context, sb *streamBatcher, rec chunk.Record, transform *query.RecordTransform) (bool, error) {
	if transform != nil {
		rec, ok := transform.Apply(ctx, rec)
		if !ok {
			return transform.Done(), nil
		}
		if err := sb.add(recordToProto(rec)); err != nil {
			return false, err
		}
		return transform.Done(), nil
	}
	return false, sb.add(recordToProto(rec))
}


// streamBatcher accumulates records and flushes them to a server stream
// in fixed-size batches.
type streamBatcher struct {
	stream *connect.ServerStream[apiv1.SearchResponse]
	batch  []*apiv1.Record
	cap    int
}

func newStreamBatcher(stream *connect.ServerStream[apiv1.SearchResponse], batchSize int) *streamBatcher {
	return &streamBatcher{stream: stream, batch: make([]*apiv1.Record, 0, batchSize), cap: batchSize}
}

func (b *streamBatcher) add(rec *apiv1.Record) error {
	b.batch = append(b.batch, rec)
	if len(b.batch) >= b.cap {
		if err := b.stream.Send(&apiv1.SearchResponse{Records: b.batch, HasMore: true}); err != nil {
			return err
		}
		b.batch = b.batch[:0]
	}
	return nil
}

func (b *streamBatcher) pending() []*apiv1.Record { return b.batch }

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

// Follow executes a query and streams matching records, continuously polling for new arrivals.
// This is a tail -f style operation that never completes until the client disconnects.
func (s *QueryServer) Follow(
	ctx context.Context,
	req *connect.Request[apiv1.FollowRequest],
	stream *connect.ServerStream[apiv1.FollowResponse],
) error {
	if s.maxFollowDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.maxFollowDuration)
		defer cancel()
	}

	eng := s.orch.MultiVaultQueryEngine()

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Pipeline queries: allow non-aggregating streaming-compatible operators in
	// follow mode. Reject stats (needs all records), sort and tail (not streaming).
	if pipeline != nil && len(pipeline.Pipes) > 0 {
		for _, pipe := range pipeline.Pipes {
			switch pipe.(type) {
			case *querylang.StatsOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("stats operator is not supported in follow mode"))
			case *querylang.SortOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("sort operator is not supported in follow mode"))
			case *querylang.TailOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("tail operator is not supported in follow mode"))
			case *querylang.SliceOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("slice operator is not supported in follow mode"))
			case *querylang.TimechartOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("timechart operator is not supported in follow mode"))
			case *querylang.BarchartOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("barchart operator is not supported in follow mode"))
			case *querylang.DonutOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("donut operator is not supported in follow mode"))
			case *querylang.HeatmapOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("heatmap operator is not supported in follow mode"))
			case *querylang.MapOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("map operator is not supported in follow mode"))
			}
		}
	}

	// Start remote follow streams for vaults on other nodes.
	remoteRecords := s.startRemoteFollows(ctx, q)

	// Local follow for vaults on this node.
	localIter := eng.Follow(ctx, q)

	// Merge local and remote records and stream to the client.
	return s.mergeFollowStreams(ctx, localIter, remoteRecords, stream)
}

// startRemoteFollows opens ForwardFollow streams to all remote nodes that own
// vaults matching the query. Returns a channel that carries records from all
// remote streams combined.
func (s *QueryServer) startRemoteFollows(ctx context.Context, q query.Query) <-chan *apiv1.Record {
	if s.remoteSearcher == nil || s.cfgStore == nil {
		return nil
	}

	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	byNode := s.remoteVaultsByNode(ctx, selectedVaults)
	if len(byNode) == 0 {
		return nil
	}

	queryExpr := q.String()
	merged := make(chan *apiv1.Record, 64)
	var wg sync.WaitGroup

	for nodeID, vaultIDs := range byNode {
		vaultStrs := make([]string, len(vaultIDs))
		for i, v := range vaultIDs {
			vaultStrs[i] = v.String()
		}

		wg.Add(1)
		go func(nodeID string, vaultStrs []string) {
			defer wg.Done()

			recCh, errCh := s.remoteSearcher.Follow(ctx, nodeID, &apiv1.ForwardFollowRequest{
				VaultIds: vaultStrs,
				Query:    queryExpr,
			})

			for {
				select {
				case rec, ok := <-recCh:
					if !ok {
						// Check for error after channel closes.
						if err := <-errCh; err != nil {
							s.logger.Warn("follow: remote stream error", "node", nodeID, "err", err)
						}
						return
					}
					select {
					case merged <- exportToRecord(rec):
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(nodeID, vaultStrs)
	}

	// Close merged channel when all remote streams end.
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}

// mergeFollowStreams interleaves local follow records with remote records
// and streams them to the client. Records are sent immediately as they arrive
// — there's no ordering guarantee in follow mode (it's real-time tailing).
func (s *QueryServer) mergeFollowStreams(
	ctx context.Context,
	localIter iter.Seq2[chunk.Record, error],
	remoteRecords <-chan *apiv1.Record,
	stream *connect.ServerStream[apiv1.FollowResponse],
) error {
	// If no remote records, just stream local.
	if remoteRecords == nil {
		return streamLocalFollow(localIter, stream)
	}

	// Both local and remote: run local in a goroutine, merge via channel.
	localCh := make(chan localFollowMsg, 64)
	go func() {
		defer close(localCh)
		for rec, err := range localIter {
			select {
			case localCh <- localFollowMsg{rec: rec, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case msg, ok := <-localCh:
			if !ok {
				return drainRemoteFollow(remoteRecords, stream)
			}
			if err := sendLocalFollowMsg(msg, stream); err != nil {
				return err
			}
		case rec, ok := <-remoteRecords:
			if !ok {
				return drainLocalFollow(localCh, stream)
			}
			if err := stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{rec}}); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

type localFollowMsg struct {
	rec chunk.Record
	err error
}

// streamLocalFollow streams all records from a local follow iterator.
func streamLocalFollow(localIter iter.Seq2[chunk.Record, error], stream *connect.ServerStream[apiv1.FollowResponse]) error {
	for rec, err := range localIter {
		if err != nil {
			return followError(err)
		}
		if err := stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{recordToProto(rec)}}); err != nil {
			return err
		}
	}
	return nil
}

// sendLocalFollowMsg sends a single local follow message to the stream.
func sendLocalFollowMsg(msg localFollowMsg, stream *connect.ServerStream[apiv1.FollowResponse]) error {
	if msg.err != nil {
		return followError(msg.err)
	}
	return stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{recordToProto(msg.rec)}})
}

// drainRemoteFollow streams remaining remote records after local closes.
func drainRemoteFollow(remoteRecords <-chan *apiv1.Record, stream *connect.ServerStream[apiv1.FollowResponse]) error {
	for rec := range remoteRecords {
		if err := stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{rec}}); err != nil {
			return err
		}
	}
	return nil
}

// drainLocalFollow streams remaining local records after remote closes.
func drainLocalFollow(localCh <-chan localFollowMsg, stream *connect.ServerStream[apiv1.FollowResponse]) error {
	for msg := range localCh {
		if err := sendLocalFollowMsg(msg, stream); err != nil {
			return err
		}
	}
	return nil
}

// followError returns nil for normal termination (context cancelled/deadline)
// or wraps the error as a connect error.
func followError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return connect.NewError(connect.CodeInternal, err)
}

// Explain returns the query execution plan without executing.
// Explains the plan for all vaults; use vault_id=X in query expression to filter.
func (s *QueryServer) Explain(
	ctx context.Context,
	req *connect.Request[apiv1.ExplainRequest],
) (*connect.Response[apiv1.ExplainResponse], error) {
	eng := s.orch.MultiVaultQueryEngine()

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	plan, err := eng.Explain(ctx, q)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.ExplainResponse{
		Chunks:      make([]*apiv1.ChunkPlan, 0, len(plan.ChunkPlans)),
		Direction:   plan.Direction,
		TotalChunks: int32(plan.TotalChunks), //nolint:gosec // G115: chunk count always fits in int32
	}
	resp.Expression = plan.Query.String()
	if !plan.Query.Start.IsZero() {
		resp.QueryStart = timestamppb.New(plan.Query.Start)
	}
	if !plan.Query.End.IsZero() {
		resp.QueryEnd = timestamppb.New(plan.Query.End)
	}

	// Append pipeline stages if the query has pipe operators.
	if pipeline != nil {
		resp.PipelineStages = buildPipelineStages(pipeline)
	}

	// Cache vault→nodeID lookups to avoid repeated config reads.
	vaultNodeCache := make(map[uuid.UUID]string)
	vaultNodeID := func(vaultID uuid.UUID) string {
		if nid, ok := vaultNodeCache[vaultID]; ok {
			return nid
		}
		// With tiered storage, vaults no longer have a NodeID.
		vaultNodeCache[vaultID] = ""
		return ""
	}

	for _, cp := range plan.ChunkPlans {
		chunkPlan := &apiv1.ChunkPlan{
			VaultId:          cp.VaultID.String(),
			ChunkId:          cp.ChunkID.String(),
			Sealed:           cp.Sealed,
			RecordCount:      int64(cp.RecordCount),
			ScanMode:         cp.ScanMode,
			EstimatedRecords: int64(cp.EstimatedScan),
			RuntimeFilters:   []string{cp.RuntimeFilter},
			Steps:            PipelineStepsToProto(cp.Pipeline),
			SkipReason:       cp.SkipReason,
			NodeId:           vaultNodeID(cp.VaultID),
		}
		if !cp.WriteStart.IsZero() {
			chunkPlan.WriteStart = timestamppb.New(cp.WriteStart)
		}
		if !cp.WriteEnd.IsZero() {
			chunkPlan.WriteEnd = timestamppb.New(cp.WriteEnd)
		}

		for _, bp := range cp.BranchPlans {
			chunkPlan.BranchPlans = append(chunkPlan.BranchPlans, &apiv1.BranchPlan{
				Expression:       bp.BranchExpr,
				Steps:            PipelineStepsToProto(bp.Pipeline),
				Skipped:          bp.Skipped,
				SkipReason:       bp.SkipReason,
				EstimatedRecords: int64(bp.EstimatedScan),
			})
		}

		resp.Chunks = append(resp.Chunks, chunkPlan)
	}

	// Fan out to remote nodes to collect their chunk plans.
	s.collectRemoteExplain(ctx, q, resp)

	return connect.NewResponse(resp), nil
}

// buildPipelineStages converts parsed pipeline operators into proto stages
// with execution metadata and human-readable notes.
func buildPipelineStages(pipeline *querylang.Pipeline) []*apiv1.QueryPipelineStage {
	stages := make([]*apiv1.QueryPipelineStage, 0, len(pipeline.Pipes))
	for _, op := range pipeline.Pipes {
		stages = append(stages, &apiv1.QueryPipelineStage{
			Operator:      pipeOpName(op),
			Description:   op.String(),
			Materializing: isMaterializing(op),
			Note:          pipeOpNote(op),
			Execution:     pipeOpExecution(op),
		})
	}
	return stages
}

// collectRemoteExplain fans out ForwardExplain RPCs to remote nodes and
// merges their chunk plans into the response.
func (s *QueryServer) collectRemoteExplain(ctx context.Context, q query.Query, resp *apiv1.ExplainResponse) {
	if s.remoteSearcher == nil || s.cfgStore == nil {
		return
	}
	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	byNode := s.remoteVaultsByNode(ctx, selectedVaults)
	queryExpr := q.String()
	for nodeID, vaultIDs := range byNode {
		vaultStrs := make([]string, len(vaultIDs))
		for i, v := range vaultIDs {
			vaultStrs[i] = v.String()
		}
		remote, err := s.remoteSearcher.Explain(ctx, nodeID, &apiv1.ForwardExplainRequest{
			Query:    queryExpr,
			VaultIds: vaultStrs,
		})
		if err != nil {
			s.logger.Warn("explain: remote node failed", "node", nodeID, "err", err)
			continue
		}
		resp.Chunks = append(resp.Chunks, remote.GetChunks()...)
		resp.TotalChunks += remote.GetTotalChunks()
	}
}

// pipeOpName returns the operator name for a PipeOp.
func pipeOpName(op querylang.PipeOp) string {
	switch op.(type) {
	case *querylang.StatsOp:
		return "stats"
	case *querylang.WhereOp:
		return "where"
	case *querylang.EvalOp:
		return "eval"
	case *querylang.SortOp:
		return "sort"
	case *querylang.HeadOp:
		return "head"
	case *querylang.TailOp:
		return "tail"
	case *querylang.SliceOp:
		return "slice"
	case *querylang.RenameOp:
		return "rename"
	case *querylang.FieldsOp:
		return "fields"
	case *querylang.TimechartOp:
		return "timechart"
	case *querylang.RawOp:
		return "raw"
	case *querylang.LookupOp:
		return "lookup"
	case *querylang.BarchartOp:
		return "barchart"
	case *querylang.DonutOp:
		return "donut"
	case *querylang.HeatmapOp:
		return "heatmap"
	case *querylang.DedupOp:
		return "dedup"
	case *querylang.MapOp:
		return "map"
	default:
		return "unknown"
	}
}

// isMaterializing returns true for pipeline operators that require full
// result materialization before producing output.
func isMaterializing(op querylang.PipeOp) bool {
	switch op.(type) {
	case *querylang.StatsOp, *querylang.TimechartOp, *querylang.SortOp,
		*querylang.TailOp, *querylang.SliceOp, *querylang.RawOp:
		return true
	default:
		return false
	}
}

// pipeOpExecution returns a short execution mode label for a pipeline operator.
func pipeOpExecution(op querylang.PipeOp) string {
	switch op.(type) {
	case *querylang.StatsOp, *querylang.TimechartOp:
		return "materializing" // runs on each node, merged on coordinator
	case *querylang.SortOp, *querylang.TailOp, *querylang.SliceOp:
		return "coordinator-only" // buffers all records on the coordinating node
	case *querylang.HeadOp:
		return "short-circuit" // stops iteration early
	case *querylang.BarchartOp, *querylang.DonutOp, *querylang.MapOp, *querylang.RawOp:
		return "render-hint" // affects presentation, not data flow
	default:
		return "streaming" // per-record, no buffering
	}
}

// pipeOpNote generates a human-readable explanation of what a pipeline operator
// does and how the engine will execute it.
func pipeOpNote(op querylang.PipeOp) string {
	switch o := op.(type) {
	case *querylang.StatsOp:
		n := fmt.Sprintf("Aggregates all matching records (%s)", aggList(o.Aggs))
		if len(o.Groups) > 0 {
			n += ", grouped by " + groupList(o.Groups)
		}
		n += ". All records must be scanned before results are produced. In a cluster, each node aggregates locally and results are merged."
		return n
	case *querylang.TimechartOp:
		n := fmt.Sprintf("Buckets records into %d time intervals", o.N)
		if o.By != "" {
			n += ", split by " + o.By
		}
		n += ". All records must be scanned. Each node runs independently, results merged on coordinator."
		return n
	case *querylang.WhereOp:
		return fmt.Sprintf("Filters records matching: %s. Applied per-record with no buffering.", o.Expr.String())
	case *querylang.EvalOp:
		fields := make([]string, len(o.Assignments))
		for i, a := range o.Assignments {
			fields[i] = a.Field
		}
		return fmt.Sprintf("Computes new fields: %s. Applied per-record.", strings.Join(fields, ", "))
	case *querylang.SortOp:
		fields := make([]string, len(o.Fields))
		for i, f := range o.Fields {
			if f.Desc {
				fields[i] = f.Name + " (desc)"
			} else {
				fields[i] = f.Name + " (asc)"
			}
		}
		return fmt.Sprintf("Sorts all results by %s. Buffers all records in memory on the coordinator.", strings.Join(fields, ", "))
	case *querylang.HeadOp:
		return fmt.Sprintf("Returns only the first %d records. Stops scanning early once the limit is reached.", o.N)
	case *querylang.TailOp:
		return fmt.Sprintf("Returns only the last %d records. All records must be scanned to find the tail.", o.N)
	case *querylang.SliceOp:
		return fmt.Sprintf("Returns records %d through %d. All records must be buffered to extract the slice.", o.Start, o.End)
	case *querylang.RenameOp:
		pairs := make([]string, len(o.Renames))
		for i, r := range o.Renames {
			pairs[i] = r.Old + " \u2192 " + r.New
		}
		return fmt.Sprintf("Renames fields: %s. Applied per-record.", strings.Join(pairs, ", "))
	case *querylang.FieldsOp:
		if o.Drop {
			return fmt.Sprintf("Drops fields: %s. Applied per-record.", strings.Join(o.Names, ", "))
		}
		return fmt.Sprintf("Keeps only fields: %s. Applied per-record.", strings.Join(o.Names, ", "))
	case *querylang.DedupOp:
		if o.Window != "" {
			return fmt.Sprintf("Removes duplicate records keyed on EventID within a %s window.", o.Window)
		}
		return "Removes duplicate records keyed on EventID within a 1s window."
	case *querylang.LookupOp:
		return fmt.Sprintf("Enriches each record by looking up %s in the %s table.", strings.Join(o.Fields, ", "), o.Table)
	case *querylang.RawOp:
		return "Forces table output format. No data transformation."
	case *querylang.BarchartOp:
		return "Renders results as a bar chart. No data transformation."
	case *querylang.DonutOp:
		return "Renders results as a donut chart. No data transformation."
	case *querylang.HeatmapOp:
		return "Renders results as a heatmap. No data transformation."
	case *querylang.MapOp:
		if o.Mode == querylang.MapChoropleth {
			return fmt.Sprintf("Renders a choropleth map by %s. No data transformation.", o.CountryField)
		}
		return fmt.Sprintf("Renders a scatter map using %s/%s coordinates. No data transformation.", o.LatField, o.LonField)
	default:
		return ""
	}
}

func aggList(aggs []querylang.AggExpr) string {
	names := make([]string, len(aggs))
	for i, a := range aggs {
		names[i] = a.DefaultAlias()
	}
	return strings.Join(names, ", ")
}

func groupList(groups []querylang.GroupExpr) string {
	names := make([]string, len(groups))
	for i, g := range groups {
		names[i] = g.String()
	}
	return strings.Join(names, ", ")
}

// GetContext returns records surrounding a specific record, searching across
// all vaults in the cluster. The anchor record is read from its owning node
// (local cursor or remote forward), but the before/after context searches
// run the full cluster-wide search path so that records from any vault appear.
func (s *QueryServer) GetContext(
	ctx context.Context,
	req *connect.Request[apiv1.GetContextRequest],
) (*connect.Response[apiv1.GetContextResponse], error) {
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}

	ref := req.Msg.Ref
	if ref == nil || ref.VaultId == "" || ref.ChunkId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ref must include vault_id, chunk_id, and pos"))
	}

	vaultID, connErr := parseUUID(ref.VaultId)
	if connErr != nil {
		return nil, connErr
	}

	chunkID, err := chunk.ParseChunkID(ref.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid chunk_id: %w", err))
	}

	// Step 1: Read the anchor record from its owning vault.
	anchor, err := s.readAnchor(ctx, vaultID, chunkID, ref.Pos)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Step 2: Collect context using the full cluster-wide search path.
	before := int(req.Msg.Before)
	after := int(req.Msg.After)
	if before == 0 {
		before = 5
	}
	if after == 0 {
		after = 5
	}

	isAnchor := func(rec *apiv1.Record) bool {
		return rec.Ref != nil &&
			rec.Ref.VaultId == ref.VaultId &&
			rec.Ref.ChunkId == ref.ChunkId &&
			rec.Ref.Pos == ref.Pos
	}

	anchorTS := anchor.GetWriteTs().AsTime()

	beforeRecs, err := s.searchContext(ctx, query.Query{
		End:       anchorTS,
		Limit:     before + 1,
		IsReverse: true,
	}, before, isAnchor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slices.Reverse(beforeRecs) // newest-first → oldest-first

	afterRecs, err := s.searchContext(ctx, query.Query{
		Start: anchorTS,
		Limit: after + 1,
	}, after, isAnchor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.GetContextResponse{
		Anchor: anchor,
		Before: beforeRecs,
		After:  afterRecs,
	}), nil
}

// readAnchor reads a single record by its ref. If the vault is local, reads
// via cursor. If remote, forwards to the owning node.
func (s *QueryServer) readAnchor(ctx context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID, pos uint64) (*apiv1.Record, error) {
	if nodeID := s.remoteNodeForVault(ctx, vaultID); nodeID != "" {
		resp, err := s.remoteSearcher.GetContext(ctx, nodeID, &apiv1.ForwardGetContextRequest{
			VaultId: vaultID.String(),
			ChunkId: chunkID.String(),
			Pos:     pos,
		})
		if err != nil {
			return nil, fmt.Errorf("remote anchor read: %w", err)
		}
		if resp.Anchor == nil {
			return nil, errors.New("remote anchor not found")
		}
		return exportToRecord(resp.Anchor), nil
	}

	eng := s.orch.MultiVaultQueryEngine()
	anchor, err := eng.ReadRecord(ctx, vaultID, chunkID, pos)
	if err != nil {
		return nil, fmt.Errorf("read anchor vault=%s chunk=%s pos=%d: %w", vaultID, chunkID, pos, err)
	}
	return recordToProto(anchor), nil
}

// searchContext runs a full cluster-wide search (local engine + remote vaults)
// and collects up to n records into a slice, skipping the anchor.
func (s *QueryServer) searchContext(
	ctx context.Context,
	q query.Query,
	n int,
	isAnchor func(*apiv1.Record) bool,
) ([]*apiv1.Record, error) {
	eng := s.orch.MultiVaultQueryEngine()
	localIter, _ := eng.Search(ctx, q, nil)
	remoteIter, _, _ := s.collectRemote(ctx, q, nil)

	reverse := q.Reverse()
	isBefore := func(a, b time.Time) bool {
		if reverse {
			return a.After(b)
		}
		return a.Before(b)
	}

	remote := drainIterToProto(remoteIter)

	ri := 0
	var result []*apiv1.Record

	for rec, err := range localIter {
		if err != nil {
			return result, err
		}
		// Drain remote records that sort before this local record.
		for ri < len(remote) && isBefore(remote[ri].GetWriteTs().AsTime(), rec.WriteTS) {
			if !isAnchor(remote[ri]) {
				result = append(result, remote[ri])
				if len(result) >= n {
					return result, nil
				}
			}
			ri++
		}
		proto := recordToProto(rec)
		if isAnchor(proto) {
			continue
		}
		result = append(result, proto)
		if len(result) >= n {
			return result, nil
		}
	}

	// Drain remaining remote records.
	for ri < len(remote) {
		if !isAnchor(remote[ri]) {
			result = append(result, remote[ri])
			if len(result) >= n {
				return result, nil
			}
		}
		ri++
	}

	return result, nil
}

// drainIterToProto collects all records from an iterator into a slice of
// proto records. Returns nil if the iterator is nil.
func drainIterToProto(it iter.Seq2[chunk.Record, error]) []*apiv1.Record {
	if it == nil {
		return nil
	}
	var out []*apiv1.Record
	for rec, err := range it {
		if err != nil {
			break
		}
		out = append(out, recordToProto(rec))
	}
	return out
}

// remoteNodeForVault returns the owning node ID if the vault is remote,
// or "" if the vault is local or lookup fails.
//
// Uses tier-level NodeID (set by the placement manager) for node assignment.
func (s *QueryServer) remoteNodeForVault(ctx context.Context, vaultID uuid.UUID) string {
	// If the vault is registered locally, it's not remote.
	if slices.Contains(s.orch.ListVaults(), vaultID) {
		return ""
	}

	if s.cfgStore == nil {
		return ""
	}

	vaultCfg, err := s.cfgStore.GetVault(ctx, vaultID)
	if err != nil || vaultCfg == nil {
		return ""
	}

	tiers, err := s.cfgStore.ListTiers(ctx)
	if err != nil {
		return ""
	}

	tierMap := make(map[uuid.UUID]*config.TierConfig, len(tiers))
	for i := range tiers {
		tierMap[tiers[i].ID] = &tiers[i]
	}

	// temporary: find the tier's NodeID to determine the owning node (until tier election).
	for _, tierID := range vaultCfg.TierIDs {
		tc := tierMap[tierID]
		if tc == nil {
			continue
		}
		if tc.NodeID != "" && tc.NodeID != s.localNodeID {
			return tc.NodeID
		}
	}
	return ""
}

// GetSyntax returns the query language keyword sets for frontend tokenization.
func (s *QueryServer) GetSyntax(
	_ context.Context,
	_ *connect.Request[apiv1.GetSyntaxRequest],
) (*connect.Response[apiv1.GetSyntaxResponse], error) {
	// Aggregation functions valid inside stats bodies.
	aggs := []string{"count", "avg", "sum", "min", "max", "bin"}
	// Combine aggs + scalar functions for the full pipeFunctions set.
	funcs := make([]string, 0, len(aggs)+len(querylang.ScalarFuncNames))
	funcs = append(funcs, aggs...)
	funcs = append(funcs, querylang.ScalarFuncNames...)

	return connect.NewResponse(&apiv1.GetSyntaxResponse{
		Directives: []string{
			"reverse", "start", "end", "last", "limit", "pos",
			"source_start", "source_end", "ingest_start", "ingest_end",
		},
		PipeKeywords:  []string{"stats", "where", "eval", "sort", "head", "tail", "slice", "rename", "fields", "timechart", "dedup", "raw", "lookup", "linechart", "barchart", "donut", "heatmap", "scatter", "map", "export"},
		PipeFunctions: funcs,
		LookupTables:  s.lookupNames,
	}), nil
}

// ValidateQuery checks whether a query expression is syntactically valid.
func (s *QueryServer) ValidateQuery(
	_ context.Context,
	req *connect.Request[apiv1.ValidateQueryRequest],
) (*connect.Response[apiv1.ValidateQueryResponse], error) {
	expr := req.Msg.Expression
	valid, msg, offset := querylang.ValidateExpression(expr)
	spans, hasPipeline := querylang.Highlight(expr, offset)

	protoSpans := make([]*apiv1.HighlightSpan, len(spans))
	for i, sp := range spans {
		protoSpans[i] = &apiv1.HighlightSpan{Text: sp.Text, Role: string(sp.Role)}
	}

	// Detect export operator in the pipeline.
	parsedPipeline := querylang.ParseExpressionPipeline(expr)
	_, hasExport := querylang.HasExportOp(parsedPipeline)

	canFollow := valid && !hasExport && (!hasPipeline || canFollowPipeline(expr))

	return connect.NewResponse(&apiv1.ValidateQueryResponse{
		Valid:        valid,
		ErrorMessage: msg,
		ErrorOffset:  int32(offset), //nolint:gosec // G115: offset fits in int32
		Spans:        protoSpans,
		Expression:   expr,
		HasPipeline:  hasPipeline,
		CanFollow:    canFollow,
		HasExport:    hasExport,
	}), nil
}

// canFollowPipeline parses the expression and checks whether its pipeline
// operators are all streamable (compatible with follow mode).
func canFollowPipeline(expr string) bool {
	pipeline := querylang.ParseExpressionPipeline(expr)
	if pipeline == nil {
		return true // no pipeline — follow is fine
	}
	return query.CanStreamPipeline(pipeline)
}

// GetPipelineFields returns available fields and completions at cursor position.
func (s *QueryServer) GetPipelineFields(
	_ context.Context,
	req *connect.Request[apiv1.GetPipelineFieldsRequest],
) (*connect.Response[apiv1.GetPipelineFieldsResponse], error) {
	fields, completions := querylang.FieldsAtCursor(
		req.Msg.Expression,
		int(req.Msg.Cursor),
		req.Msg.BaseFields,
	)
	return connect.NewResponse(&apiv1.GetPipelineFieldsResponse{
		Fields:      fields,
		Completions: completions,
	}), nil
}

// protoToQuery converts a proto Query to the internal query.Query type.
// If the Expression field is set, it is parsed server-side and takes
// precedence over the legacy Tokens/KvPredicates fields.
// Returns the pipeline if the expression contains pipe operators (e.g. "| stats count").
func protoToQuery(pq *apiv1.Query) (query.Query, *querylang.Pipeline, error) {
	if pq == nil {
		return query.Query{}, nil, nil
	}

	// If Expression is set, parse it server-side (same logic as repl/parse.go).
	// Proto-level fields (Limit, Start, End) override expression-level values
	// when set, so the frontend can control page size without injecting limit=
	// into the expression string.
	if pq.Expression != "" {
		q, pipeline, err := parseExpression(pq.Expression)
		if err != nil {
			return q, nil, err
		}
		if pq.Limit > 0 && q.Limit == 0 {
			q.Limit = int(pq.Limit)
		}
		if pq.Start != nil && q.Start.IsZero() {
			q.Start = pq.Start.AsTime()
		}
		if pq.End != nil && q.End.IsZero() {
			q.End = pq.End.AsTime()
		}
		return q, pipeline, nil
	}

	// Legacy path: use structured Tokens/KvPredicates fields.
	q := query.Query{
		Tokens:        pq.Tokens,
		Limit:         int(pq.Limit),
		ContextBefore: int(pq.ContextBefore),
		ContextAfter:  int(pq.ContextAfter),
	}

	if pq.Start != nil {
		q.Start = pq.Start.AsTime()
	}
	if pq.End != nil {
		q.End = pq.End.AsTime()
	}

	if len(pq.KvPredicates) > 0 {
		q.KV = make([]query.KeyValueFilter, len(pq.KvPredicates))
		for i, kv := range pq.KvPredicates {
			q.KV[i] = query.KeyValueFilter{Key: kv.Key, Value: kv.Value}
		}
	}

	return q, nil, nil
}

const maxExpressionLength = 4096

// parseExpression parses a raw query expression string into a Query and optional Pipeline.
// Control arguments (start=, end=, limit=) are extracted; the remainder
// ParseExpression parses a query expression string into a Query and optional Pipeline.
// Exported for use by the search executor in cluster forwarding.
func ParseExpression(expr string) (query.Query, *querylang.Pipeline, error) {
	return parseExpression(expr)
}

// is parsed through the pipeline parser. If the expression contains pipe
// operators (e.g. "| stats count"), the pipeline is returned; otherwise
// only the filter expression is set on the query.
func parseExpression(expr string) (query.Query, *querylang.Pipeline, error) {
	if len(expr) > maxExpressionLength {
		return query.Query{}, nil, fmt.Errorf("expression too long: %d bytes (max %d)", len(expr), maxExpressionLength)
	}
	expr = querylang.StripComments(expr)
	parts := strings.Fields(expr)
	if len(parts) == 0 {
		return query.Query{}, nil, nil
	}

	var q query.Query
	var filterParts []string

	for _, part := range parts {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			filterParts = append(filterParts, part)
			continue
		}
		consumed, err := applyDirective(&q, k, v)
		if err != nil {
			return q, nil, err
		}
		if !consumed {
			filterParts = append(filterParts, part)
		}
	}

	if len(filterParts) == 0 {
		return q, nil, nil
	}

	pipeline, err := querylang.ParsePipeline(strings.Join(filterParts, " "))
	if err != nil {
		return q, nil, fmt.Errorf("parse error: %w", err)
	}
	q.BoolExpr = pipeline.Filter
	if len(pipeline.Pipes) > 0 {
		return q, pipeline, nil
	}
	return q, nil, nil
}

func applyDirective(q *query.Query, k, v string) (bool, error) {
	switch k {
	case "reverse":
		q.IsReverse = v == "true"
		return true, nil
	case "start":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid start time: %w", err)
		}
		q.Start = t
		return true, nil
	case "end":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid end time: %w", err)
		}
		q.End = t
		return true, nil
	case "last":
		d, err := parseDuration(v)
		if err != nil {
			return false, fmt.Errorf("invalid last duration: %w", err)
		}
		now := time.Now()
		q.Start = now.Add(-d)
		q.End = now
		return true, nil
	case "source_start":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid source_start time: %w", err)
		}
		q.SourceStart = t
		return true, nil
	case "source_end":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid source_end time: %w", err)
		}
		q.SourceEnd = t
		return true, nil
	case "ingest_start":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid ingest_start time: %w", err)
		}
		q.Start = t
		return true, nil
	case "ingest_end":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid ingest_end time: %w", err)
		}
		q.End = t
		return true, nil
	case "limit":
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
			return false, fmt.Errorf("invalid limit: %w", err)
		}
		q.Limit = n
		return true, nil
	case "pos":
		var n uint64
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
			return false, fmt.Errorf("invalid pos: %w", err)
		}
		q.Pos = &n
		return true, nil
	case "order":
		switch v {
		case "ingest_ts":
			q.OrderBy = query.OrderByIngestTS
		case "source_ts":
			q.OrderBy = query.OrderBySourceTS
		default:
			return false, fmt.Errorf("invalid order: %s (use ingest_ts or source_ts)", v)
		}
		return true, nil
	default:
		return false, nil
	}
}

// parseDuration parses a duration string like "5m", "1h", or "3d".
// Extends time.ParseDuration with support for day suffixes.
func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		var days int
		if _, err := fmt.Sscanf(s, "%dd", &days); err == nil && days > 0 {
			return time.Duration(days) * 24 * time.Hour, nil
		}
	}
	return time.ParseDuration(s)
}

// parseTime parses a time string in RFC3339 format or as a Unix timestamp.
func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	var unix int64
	if n, err := fmt.Sscanf(s, "%d", &unix); err == nil && n == 1 && strconv.FormatInt(unix, 10) == s {
		return time.Unix(unix, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %s (use RFC3339 or Unix timestamp)", s)
}

// PipelineStepsToProto converts internal PipelineSteps to proto.
// Exported for use by the explain executor in cluster forwarding.
func PipelineStepsToProto(steps []query.PipelineStep) []*apiv1.PipelineStep {
	out := make([]*apiv1.PipelineStep, len(steps))
	for i, step := range steps {
		out[i] = &apiv1.PipelineStep{
			Name:           step.Index,
			InputEstimate:  int64(step.PositionsBefore),
			OutputEstimate: int64(step.PositionsAfter),
			Action:         step.Action,
			Reason:         step.Reason,
			Detail:         step.Details,
			Predicate:      step.Predicate,
		}
	}
	return out
}

// tableResultToProto converts an internal TableResult to the proto type.
func tableResultToProto(result *query.TableResult, pipeline *querylang.Pipeline) *apiv1.TableResult {
	rows := make([]*apiv1.TableRow, len(result.Rows))
	for i, row := range result.Rows {
		rows[i] = &apiv1.TableRow{Values: row}
	}

	// Determine result type from pipeline: timeseries if bin() or timechart
	// is present, but raw forces plain table.
	resultType := "table"
	hasRaw := false
	var vizOp querylang.PipeOp
	for _, pipe := range pipeline.Pipes {
		if _, ok := pipe.(*querylang.RawOp); ok {
			hasRaw = true
		}
		if _, ok := pipe.(*querylang.TimechartOp); ok {
			resultType = "timechart"
		}
		if stats, ok := pipe.(*querylang.StatsOp); ok {
			for _, g := range stats.Groups {
				if g.Bin != nil {
					resultType = "timeseries"
					break
				}
			}
		}
		switch pipe.(type) {
		case *querylang.LinechartOp, *querylang.BarchartOp, *querylang.DonutOp, *querylang.HeatmapOp, *querylang.ScatterOp, *querylang.MapOp:
			vizOp = pipe
		}
	}
	if hasRaw {
		resultType = "raw"
	}

	// Explicit viz operator overrides the result type if validation passes.
	// On validation failure, falls back to whatever resultType was computed above.
	if vizOp != nil && !hasRaw {
		if vizType := query.ValidateVizOp(vizOp, result); vizType != "" {
			resultType = vizType
		}
	}

	// Auto-detect visualization when no explicit operator was given.
	if resultType == "table" && vizOp == nil {
		if vizType := query.AutoDetectVizType(result); vizType != "" {
			resultType = vizType
		}
	}

	return &apiv1.TableResult{
		Columns:    result.Columns,
		Rows:       rows,
		Truncated:  result.Truncated,
		ResultType: resultType,
	}
}

// recordToProto converts an internal Record to the proto type.
func recordToProto(rec chunk.Record) *apiv1.Record {
	r := &apiv1.Record{
		IngestTs:   timestamppb.New(rec.IngestTS),
		WriteTs:    timestamppb.New(rec.WriteTS),
		Attrs:      rec.Attrs,
		Raw:        rec.Raw,
		IngestSeq:  rec.EventID.IngestSeq,
		IngesterId: rec.EventID.IngesterID[:],
		Ref: &apiv1.RecordRef{
			ChunkId: rec.Ref.ChunkID.String(),
			Pos:     rec.Ref.Pos,
			VaultId: rec.VaultID.String(),
		},
	}
	if !rec.SourceTS.IsZero() {
		r.SourceTs = timestamppb.New(rec.SourceTS)
	}
	return r
}

// ProtoToResumeToken converts a proto resume token to the internal type.
func ProtoToResumeToken(data []byte) (*query.ResumeToken, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var protoToken apiv1.ResumeToken
	if err := proto.Unmarshal(data, &protoToken); err != nil {
		return nil, fmt.Errorf("unmarshal resume token: %w", err)
	}

	token := &query.ResumeToken{}
	if len(protoToken.VaultTokens) > 0 {
		token.VaultTokens = make(map[uuid.UUID][]byte, len(protoToken.VaultTokens))
		for vidStr, tokenData := range protoToken.VaultTokens {
			vid, err := uuid.Parse(vidStr)
			if err != nil {
				continue
			}
			token.VaultTokens[vid] = tokenData
		}
	}
	if protoToken.FrozenStart != nil {
		token.FrozenStart = protoToken.FrozenStart.AsTime()
	}
	if protoToken.FrozenEnd != nil {
		token.FrozenEnd = protoToken.FrozenEnd.AsTime()
	}
	return token, nil
}

// VaultTokenToPositions deserializes a per-vault opaque token into Positions.
// Used by searchDirect to extract local vault resume state.
func VaultTokenToPositions(data []byte) ([]query.MultiVaultPosition, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var inner apiv1.InnerVaultToken
	if err := proto.Unmarshal(data, &inner); err != nil {
		return nil, err
	}
	positions := make([]query.MultiVaultPosition, len(inner.Positions))
	for i, pos := range inner.Positions {
		chunkID, err := chunk.ParseChunkID(pos.ChunkId)
		if err != nil {
			return nil, err
		}
		vaultID, err := uuid.Parse(pos.VaultId)
		if err != nil {
			return nil, err
		}
		mvp := query.MultiVaultPosition{
			VaultID:  vaultID,
			ChunkID:  chunkID,
			Position: pos.Position,
		}
		if pos.ResumeTs != nil {
			mvp.ResumeTS = pos.ResumeTs.AsTime()
		}
		positions[i] = mvp
	}
	return positions, nil
}

// PositionsToVaultToken serializes Positions into a per-vault opaque token.
func PositionsToVaultToken(positions []query.MultiVaultPosition) []byte {
	if len(positions) == 0 {
		return nil
	}
	inner := &apiv1.InnerVaultToken{
		Positions: make([]*apiv1.VaultPosition, len(positions)),
	}
	for i, pos := range positions {
		vp := &apiv1.VaultPosition{
			VaultId:  pos.VaultID.String(),
			ChunkId:  pos.ChunkID.String(),
			Position: pos.Position,
		}
		if !pos.ResumeTS.IsZero() {
			vp.ResumeTs = timestamppb.New(pos.ResumeTS)
		}
		inner.Positions[i] = vp
	}
	data, err := proto.Marshal(inner)
	if err != nil {
		return nil
	}
	return data
}

// ResumeTokenToProto converts an internal resume token to proto bytes.
func ResumeTokenToProto(token *query.ResumeToken) []byte {
	if token == nil || (len(token.Positions) == 0 && len(token.VaultTokens) == 0) {
		return nil
	}

	protoToken := &apiv1.ResumeToken{}

	// If there are raw Positions (from eng.Search local), serialize them
	// as per-vault tokens grouped by vault ID.
	if len(token.Positions) > 0 {
		byVault := make(map[uuid.UUID][]query.MultiVaultPosition)
		for _, pos := range token.Positions {
			byVault[pos.VaultID] = append(byVault[pos.VaultID], pos)
		}
		if token.VaultTokens == nil {
			token.VaultTokens = make(map[uuid.UUID][]byte)
		}
		for vid, positions := range byVault {
			token.VaultTokens[vid] = PositionsToVaultToken(positions)
		}
	}

	if len(token.VaultTokens) > 0 {
		protoToken.VaultTokens = make(map[string][]byte, len(token.VaultTokens))
		for vid, tokenData := range token.VaultTokens {
			protoToken.VaultTokens[vid.String()] = tokenData
		}
	}
	if !token.FrozenStart.IsZero() {
		protoToken.FrozenStart = timestamppb.New(token.FrozenStart)
	}
	if !token.FrozenEnd.IsZero() {
		protoToken.FrozenEnd = timestamppb.New(token.FrozenEnd)
	}

	data, err := proto.Marshal(protoToken)
	if err != nil {
		return nil // Should not happen with valid data
	}
	return data
}
