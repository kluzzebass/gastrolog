package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"iter"
	"log/slog"
	"maps"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/lookup"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
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
	cfgStore          system.Store
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
func NewQueryServer(orch *orchestrator.Orchestrator, cfgStore system.Store, remoteSearcher RemoteSearcher, localNodeID string, lookupResolver lookup.Resolver, lookupNames []string, queryTimeout, maxFollowDuration time.Duration, maxResultCount int64, logger *slog.Logger) *QueryServer {
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

	eng := s.orch.PrimaryTierQueryEngine()
	if s.lookupResolver != nil {
		eng.SetLookupResolver(s.lookupResolver)
	}

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return errInvalidArg(err)
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
			return errInvalidArg(err)
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
	localHist := HistogramToProto(eng.ComputeHistogram(ctx, q, 50))
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
				token.VaultTokens = make(map[glid.GLID][]byte)
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

	var streamedHistogram *streamedHistogramBuilder
	multiNodeVault := false
	if s.cfgStore != nil && s.remoteSearcher != nil {
		selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
		byNodeForBuilder := s.remoteVaultsByNode(ctx, selectedVaults)
		multiNodeVault = hasMultiNodeVault(byNodeForBuilder)
	}
	start, end, hasRange := normalizedRange(q.Start, q.End)
	if transform == nil && multiNodeVault && hasRange {
		hist, _, err := s.computeDedupHistogram(ctx, eng, q, start, end)
		if err == nil {
			histogram = hist
		}
	}

	return s.mergeAndStream(ctx, localIter, getToken, remoteIter, q.OrderBy, q.Reverse(), transform, histogram, streamedHistogram, stream)
}

// splitResumeToken separates a unified resume token into local positions
// (for eng.Search) and remote opaque blobs (for collectRemote).
func (s *QueryServer) splitResumeToken(resume *query.ResumeToken) (*query.ResumeToken, map[glid.GLID][]byte) {
	if resume == nil || len(resume.VaultTokens) == 0 {
		return nil, nil
	}

	// No local-vault skip — a vault may have some tiers local and others
	// remote. Both need to be searched. The ForwardSearch handler on the
	// remote node only searches its LOCAL tiers, so no double-counting.

	remoteTokens := make(map[glid.GLID][]byte)
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
		return errInvalidArg(err)
	default:
		return errInternal(err)
	}
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
	streamedHistogram *streamedHistogramBuilder,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	sb := newStreamBatcher(stream, 100)

	if remoteIter != nil {
		// Two-way sorted merge of local and remote iterators.
		if err := mergeIterators(ctx, sb, localIter, remoteIter, orderBy, reverse, transform, streamedHistogram); err != nil {
			return err
		}
	} else {
		// Fast path: no remote results, just stream local.
		if err := streamLocal(ctx, sb, localIter, transform, streamedHistogram); err != nil {
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
	finalHistogram := histogram
	if streamedHistogram != nil {
		finalHistogram = streamedHistogram.toProto()
	}

	return stream.Send(&apiv1.SearchResponse{
		Records:     sb.pending(),
		ResumeToken: tokenBytes,
		HasMore:     len(tokenBytes) > 0,
		Histogram:   finalHistogram,
	})
}

// streamLocal streams local iterator results through the batcher.
func streamLocal(ctx context.Context, sb *streamBatcher, localIter iter.Seq2[chunk.Record, error], transform *query.RecordTransform, streamedHistogram *streamedHistogramBuilder) error {
	for rec, err := range localIter {
		if err != nil {
			return mapSearchError(err)
		}
		done, emitErr := emitRecord(ctx, sb, rec, transform, streamedHistogram)
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
	streamedHistogram *streamedHistogramBuilder,
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

		done, err := emitRecord(ctx, sb, rec, transform, streamedHistogram)
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
func emitRecord(ctx context.Context, sb *streamBatcher, rec chunk.Record, transform *query.RecordTransform, streamedHistogram *streamedHistogramBuilder) (bool, error) {
	if streamedHistogram != nil {
		streamedHistogram.add(rec)
	}
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

func normalizedRange(start, end time.Time) (time.Time, time.Time, bool) {
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

func hasMultiNodeVault(byNode map[string][]glid.GLID) bool {
	if len(byNode) <= 1 {
		return false
	}
	counts := make(map[glid.GLID]int)
	for _, vaultIDs := range byNode {
		for _, id := range vaultIDs {
			counts[id]++
			if counts[id] > 1 {
				return true
			}
		}
	}
	return false
}

type dedupHistogramStats struct {
	histTotal    int64
	uniqueCount  int64
	dedupedCount int64
}

func (s *QueryServer) computeDedupHistogram(ctx context.Context, eng *query.Engine, q query.Query, start, end time.Time) ([]*apiv1.HistogramBucket, dedupHistogramStats, error) {
	qHist := q
	qHist.Limit = 0
	builder := newStreamedHistogramBuilder(start, end, 50)

	localIter, _ := eng.Search(ctx, qHist, nil)
	remoteIter, _, _ := s.collectRemote(ctx, qHist, nil)

	if remoteIter == nil {
		for rec, err := range localIter {
			if err != nil {
				return nil, dedupHistogramStats{}, err
			}
			builder.add(rec)
		}
	} else {
		if err := consumeMergedForHistogram(localIter, remoteIter, q.OrderBy, q.Reverse(), builder); err != nil {
			return nil, dedupHistogramStats{}, err
		}
	}

	return builder.toProto(), dedupHistogramStats{
		histTotal:    builder.totalCount(),
		uniqueCount:  builder.uniqueCount,
		dedupedCount: builder.dedupedCount,
	}, nil
}

func consumeMergedForHistogram(
	localIter, remoteIter iter.Seq2[chunk.Record, error],
	orderBy query.OrderBy,
	reverse bool,
	builder *streamedHistogramBuilder,
) error {
	localNext, stopLocal := iter.Pull2(localIter)
	defer stopLocal()
	remoteNext, stopRemote := iter.Pull2(remoteIter)
	defer stopRemote()

	localRec, localErr, localOK := localNext()
	remoteRec, remoteErr, remoteOK := remoteNext()

	isBefore := func(a, b time.Time) bool {
		if reverse {
			return a.After(b)
		}
		return a.Before(b)
	}

	for localOK || remoteOK {
		if localErr != nil {
			return localErr
		}
		if remoteErr != nil {
			return remoteErr
		}
		switch {
		case !localOK:
			builder.add(remoteRec)
			remoteRec, remoteErr, remoteOK = remoteNext()
		case !remoteOK:
			builder.add(localRec)
			localRec, localErr, localOK = localNext()
		default:
			localTS := orderBy.RecordTS(localRec)
			remoteTS := orderBy.RecordTS(remoteRec)
			if isBefore(localTS, remoteTS) {
				builder.add(localRec)
				localRec, localErr, localOK = localNext()
			} else {
				builder.add(remoteRec)
				remoteRec, remoteErr, remoteOK = remoteNext()
			}
		}
	}

	return nil
}

type streamedHistogramBuilder struct {
	start        time.Time
	end          time.Time
	bucketWidth  time.Duration
	counts       []int64
	groupCounts  []map[string]int64
	seen         map[chunk.EventID]struct{}
	uniqueCount  int64
	dedupedCount int64
}

func newStreamedHistogramBuilder(start, end time.Time, numBuckets int) *streamedHistogramBuilder {
	if numBuckets <= 0 {
		numBuckets = 50
	}
	width := end.Sub(start) / time.Duration(numBuckets)
	if width <= 0 {
		width = time.Second
	}
	groupCounts := make([]map[string]int64, numBuckets)
	for i := range groupCounts {
		groupCounts[i] = make(map[string]int64)
	}
	return &streamedHistogramBuilder{
		start:       start,
		end:         end,
		bucketWidth: width,
		counts:      make([]int64, numBuckets),
		groupCounts: groupCounts,
		seen:        make(map[chunk.EventID]struct{}),
	}
}

func (h *streamedHistogramBuilder) add(rec chunk.Record) {
	if rec.IngestTS.Before(h.start) || !rec.IngestTS.Before(h.end) {
		return
	}
	if rec.EventID != (chunk.EventID{}) {
		if _, exists := h.seen[rec.EventID]; exists {
			h.dedupedCount++
			return
		}
		h.seen[rec.EventID] = struct{}{}
	}
	idx := int(rec.IngestTS.Sub(h.start) / h.bucketWidth)
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	h.counts[idx]++
	if lvl := rec.Attrs["level"]; lvl != "" {
		h.groupCounts[idx][lvl]++
	}
	h.uniqueCount++
}

func (h *streamedHistogramBuilder) totalCount() int64 {
	var total int64
	for _, c := range h.counts {
		total += c
	}
	return total
}

func (h *streamedHistogramBuilder) toProto() []*apiv1.HistogramBucket {
	out := make([]*apiv1.HistogramBucket, len(h.counts))
	for i := range h.counts {
		ts := h.start.Add(h.bucketWidth * time.Duration(i)).UnixMilli()
		gc := h.groupCounts[i]
		var known int64
		for _, v := range gc {
			known += v
		}
		if other := h.counts[i] - known; other > 0 {
			if gc == nil {
				gc = make(map[string]int64)
			}
			gc["other"] = other
		}
		out[i] = &apiv1.HistogramBucket{
			TimestampMs: ts,
			Count:       h.counts[i],
			GroupCounts: gc,
		}
	}
	return out
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
