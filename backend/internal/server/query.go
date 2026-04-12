package server

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/lookup"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"

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
