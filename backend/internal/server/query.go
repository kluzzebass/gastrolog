package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"iter"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/lookup"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/safeutf8"
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
	serverStart := time.Now()
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}

	eng := s.orch.LeaderVaultQueryEngine()
	if s.lookupResolver != nil {
		eng.SetLookupResolver(s.lookupResolver)
	}

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return errInvalidArg(err)
	}

	// Resolve unbounded queries (last=all, no time directive) to concrete
	// bounds on the coordinator before fan-out. Without this, every node
	// independently calls deriveTimeRange against its own local chunk view
	// and produces non-overlapping bucket grids that mergeHistogramBuckets
	// cannot reconcile — the histogram ends up split between two unrelated
	// time ranges. last=5m / last=1h already pre-resolve via applyDirective
	// (using the coordinator's clock), so they get aligned grids for free;
	// this closes the gap for the unbounded case.
	q = s.resolveUnboundedQuery(ctx, q)

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
			return s.searchDirect(ctx, eng, q, req.Msg.ResumeToken, transform, serverStart, stream)
		}
		// Aggregating / full-materialization pipeline (stats, timechart,
		// sort, tail, slice, raw).
		return s.searchPipeline(ctx, eng, q, pipeline, stream)
	}

	return s.searchDirect(ctx, eng, q, req.Msg.ResumeToken, nil, serverStart, stream)
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
	serverStart time.Time,
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

	// Capture the frozen bounds BEFORE applying the highwater. The frozen
	// window is what gets written back to the next page's resume token; if
	// we captured after narrowing, each page would tighten the window
	// cumulatively until it collapses. The histogram also uses the frozen
	// bounds — narrowing them per page makes the histogram report fewer
	// records as the user scrolls.
	frozenStart, frozenEnd := q.Start, q.End

	// Apply the highwater TS as an exclusive boundary for this page's
	// search ONLY (not the histogram). With reverse=true narrow q.End to
	// the highwater (records strictly older); with forward narrow q.Start
	// (records strictly newer). This is what makes pagination survive
	// chunk-lifecycle transitions during a scroll: even if every per-chunk
	// position references a chunk that vanished, the time bound prevents
	// re-emitting records the client already saw.
	histogramQ := q
	if resume != nil {
		narrowQueryByHighwater(&q, resume.HighwaterTS)
	}

	localResume, remoteTokens := s.splitResumeToken(resume)

	// Collect remote results as a streaming iterator. The remote also
	// computes a histogram inside its forwardSearchAfterParse, but on
	// resume pages we discard it — the histogram is computed once on
	// page 1 and the client keeps it across pagination.
	remoteIter, remoteHist, _ := s.collectRemote(ctx, q, remoteTokens)

	// Histogram is computed only on the FIRST page of a paginated search.
	// Subsequent pages return an empty histogram; the client keeps the
	// page-1 histogram unchanged for the lifetime of the scroll. This is
	// correct (the histogram is a function of the frozen window, which
	// doesn't change between pages) and avoids two thorny problems:
	//   1. Recomputing on every page burns CPU on large windows.
	//   2. The narrowed search window would otherwise leak into the
	//      histogram, making it report fewer records as the user scrolls.
	var histogram []*apiv1.HistogramBucket
	if resume == nil {
		if s.histogramFullyLocal(ctx, histogramQ) {
			localEng := s.orch.LocalVaultQueryEngine()
			if s.lookupResolver != nil {
				localEng.SetLookupResolver(s.lookupResolver)
			}
			histogram = HistogramToProto(localEng.ComputeHistogram(ctx, histogramQ, 50))
		} else {
			localHist := HistogramToProto(eng.ComputeHistogram(ctx, histogramQ, 50))
			histogram = mergeHistogramBuckets(localHist, remoteHist)
		}
	}

	localIter, getLocalToken := eng.Search(ctx, q, localResume)

	// Build the resume token from LOCAL positions + the merge-level highwater
	// only. Remote opaque position tokens are deliberately not propagated: a
	// remote node sends up to q.Limit records over the wire, but the
	// merge-level cap may emit only a subset to the client. The remote's
	// token would then say "past N records" while only M < N were displayed
	// — re-using it on the next page silently skips records [M+1..N] from
	// that remote vault. The HighwaterTS exclusive time bound is sufficient
	// to advance the remote correctly: each subsequent page narrows q.End,
	// and the remote re-searches from the window edge with the tighter
	// upper bound, so already-displayed records are filtered out and
	// not-yet-displayed records are reachable.
	getToken := func() *query.ResumeToken {
		token := getLocalToken()
		if token == nil {
			token = &query.ResumeToken{}
		}
		hasPositions := len(token.Positions) > 0
		hasVaultTokens := len(token.VaultTokens) > 0
		if !hasPositions && !hasVaultTokens && token.HighwaterTS.IsZero() {
			return nil
		}
		token.FrozenStart = frozenStart
		token.FrozenEnd = frozenEnd
		return token
	}

	// computeDedupHistogram (record-iterating EventID dedup) is no longer
	// needed for cross-tier transition double-counting — that's handled at
	// the index level via per-chunk IngestTSMonotonic dispatch +
	// ScanTSBounds + on-disk TS index rank arithmetic. The local histogram
	// path above computes correct counts directly. Skipping the dedup
	// avoids iterating ALL records per histogram query. See gastrolog-66b7x.
	var streamedHistogram *streamedHistogramBuilder

	return s.mergeAndStream(ctx, localIter, getToken, remoteIter, q.OrderBy, q.Reverse(), q.Limit, transform, histogram, streamedHistogram, serverStart, stream)
}

// splitResumeToken separates a unified resume token into local positions
// (for eng.Search) and remote opaque blobs (for collectRemote).
//
// Post-vault-refactor (gastrolog-257l7): all keys in VaultTokens are
// vault IDs — the local query engine's leaderTierRegistry now emits
// positions tagged by vault ID instead of tier ID. The split is a
// straight membership check against the local-leader vault set; no more
// dual-ID-space dispatch.
func (s *QueryServer) splitResumeToken(resume *query.ResumeToken) (*query.ResumeToken, map[glid.GLID][]byte) {
	if resume == nil || len(resume.VaultTokens) == 0 {
		return nil, nil
	}

	localVaults := s.orch.LocalLeaderVaultIDs()

	remoteTokens := make(map[glid.GLID][]byte)
	var localPositions []query.MultiVaultPosition
	for vid, tokenData := range resume.VaultTokens {
		if localVaults[vid] {
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

// narrowQueryByHighwater applies a resume-token highwater as an exclusive
// time bound on q. With reverse=true the highwater becomes the upper bound
// (records strictly older); with forward it becomes the lower bound
// (records strictly newer). No-op when highwater is zero or already
// outside the existing bound.
//
// The narrowed bound matches the active sort key: by default we narrow
// IngestTS (q.Start/q.End), but when sorting by SourceTS we narrow
// SourceStart/SourceEnd instead. The chunkMatchesQuery filter consults
// both axes, so narrowing the wrong axis would either fail to exclude
// already-emitted records (causing duplicates across pages) or exclude
// records that should still be reachable.
func narrowQueryByHighwater(q *query.Query, highwater time.Time) {
	if highwater.IsZero() {
		return
	}
	var lower, upper *time.Time
	if q.OrderBy == query.OrderBySourceTS {
		lower, upper = &q.SourceStart, &q.SourceEnd
	} else {
		lower, upper = &q.Start, &q.End
	}
	if q.Reverse() {
		if upper.IsZero() || highwater.Before(*upper) {
			*upper = highwater
		}
		return
	}
	if lower.IsZero() || highwater.After(*lower) {
		*lower = highwater
	}
}

// buildResumeTokenBytes serializes the resume token for the response,
// overriding the engine-derived highwater with the merge-level one when
// the merge advanced strictly further. The merge-level highwater is the
// only value that observes records emitted from BOTH local and remote
// iterators — the engine alone cannot see remote-sourced records.
//
// When lastLocalSet is true, the engine's per-chunk Positions are
// replaced with a single position pointing at the last record the merge
// actually displayed. This corrects the "pull-ahead" mismatch: the
// engine's lastRefs tracks the most recent value yielded by the iter,
// but a sorted merge always has one record pulled ahead per source for
// comparison, so the engine's positions overshoot what the client saw.
func buildResumeTokenBytes(transform *query.RecordTransform, getToken func() *query.ResumeToken, mergeHighwater time.Time, reverse bool, lastLocalSet bool, lastLocalRec chunk.Record, mergeInvolved bool) []byte {
	if transform != nil && transform.Done() {
		return nil
	}
	token := getToken()
	// On the merge path, when local emits zero records (e.g. remote-only
	// search) the engine returns nil. The remote may still hold records
	// below the merge-level highwater that need a continuation token.
	// Synthesize an empty token to carry the highwater forward.
	//
	// On the streamLocal path, the engine is authoritative — natural
	// exhaustion means no more records exist. Do NOT synthesize a token
	// from highwater alone, or the client will auto-paginate against a
	// query that has nothing left to give and re-fetch already-displayed
	// records.
	if token == nil && mergeInvolved && (lastLocalSet || !mergeHighwater.IsZero()) {
		token = &query.ResumeToken{}
	}
	if token == nil {
		return nil
	}
	if lastLocalSet {
		token.Positions = []query.MultiVaultPosition{{
			VaultID:  lastLocalRec.VaultID,
			ChunkID:  lastLocalRec.Ref.ChunkID,
			Position: lastLocalRec.Ref.Pos,
		}}
		token.VaultTokens = nil
	}
	// mergeHighwater is the TS of the last record emitted to the client. It
	// is authoritative — the engine's own highwaterTS reflects records the
	// iter yielded, but the sorted merge pulls one record ahead per source
	// for comparison, so the engine's value is one record past what the
	// client saw. Always override with mergeHighwater when set.
	if !mergeHighwater.IsZero() {
		token.HighwaterTS = mergeHighwater
	}
	return ResumeTokenToProto(token)
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
//
// limit is the merge-level cap on records emitted to the client. The local
// and remote iterators each apply q.Limit independently — without a cap
// here, the merge would concatenate up to 2*q.Limit records, and when the
// two sources cover non-overlapping time ranges (e.g. a hot vault with the
// last 3min and a warm vault with retention-routed older records) the
// concatenation produces a visible temporal gap at the boundary between
// the two slices. See "merge-level limit" in the design notes.
func (s *QueryServer) mergeAndStream(
	ctx context.Context,
	localIter iter.Seq2[chunk.Record, error],
	getToken func() *query.ResumeToken,
	remoteIter iter.Seq2[chunk.Record, error],
	orderBy query.OrderBy,
	reverse bool,
	limit int,
	transform *query.RecordTransform,
	histogram []*apiv1.HistogramBucket,
	streamedHistogram *streamedHistogramBuilder,
	serverStart time.Time,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	sb := newStreamBatcher(stream, 100)
	// Track the IngestTS of the last record actually emitted by this server
	// (across both local and remote sources). Used to override the engine's
	// own highwater on the resume token — the engine only sees its local
	// emissions, so when records come from a remote iterator the engine's
	// highwater stays zero and the bound on the next page would be lost.
	var mergeHighwater time.Time
	// Track the last local record actually emitted by the merge. The
	// engine's own lastRefs is updated BEFORE the iter yields, but a sorted
	// merge pulls one record ahead from each source to compare timestamps
	// — so the engine's lastRefs ends up at the record AFTER the last one
	// the merge actually displayed. Using it verbatim in the resume token
	// causes the next page to skip a record. We capture the last local rec
	// here to override the over-advanced engine positions.
	var lastLocalRec chunk.Record
	var lastLocalSet bool
	// On the streamLocal path the engine is the only source — natural
	// exhaustion means there are no more records anywhere, so we must
	// not synthesize a continuation token from mergeHighwater. On the
	// merge path a remote source may still hold records below the
	// highwater, so the highwater is a meaningful continuation cue
	// even when local exhausted.
	var mergeInvolved bool

	if remoteIter != nil {
		mergeInvolved = true
		// Two-way sorted merge of local and remote iterators.
		captured := chunk.Record{}
		if err := mergeIterators(ctx, sb, localIter, remoteIter, orderBy, reverse, limit, transform, streamedHistogram, &mergeHighwater, &captured); err != nil {
			return err
		}
		if !captured.IngestTS.IsZero() {
			lastLocalRec = captured
			lastLocalSet = true
		}
	} else {
		// Fast path: no remote results, just stream local.
		if err := streamLocal(ctx, sb, localIter, transform, streamedHistogram, &mergeHighwater); err != nil {
			return err
		}
	}

	// Build resume token from local state only (remote is fully streamed).
	tokenBytes := buildResumeTokenBytes(transform, getToken, mergeHighwater, reverse, lastLocalSet, lastLocalRec, mergeInvolved)
	finalHistogram := histogram
	if streamedHistogram != nil {
		finalHistogram = streamedHistogram.toProto()
	}

	return stream.Send(&apiv1.SearchResponse{
		Records:         sb.pending(),
		ResumeToken:     tokenBytes,
		HasMore:         len(tokenBytes) > 0,
		Histogram:       finalHistogram,
		ServerElapsedMs: time.Since(serverStart).Milliseconds(),
	})
}

// streamLocal streams local iterator results through the batcher.
func streamLocal(ctx context.Context, sb *streamBatcher, localIter iter.Seq2[chunk.Record, error], transform *query.RecordTransform, streamedHistogram *streamedHistogramBuilder, highwater *time.Time) error {
	for rec, err := range localIter {
		if err != nil {
			return mapSearchError(err)
		}
		done, emitErr := emitRecord(ctx, sb, rec, transform, streamedHistogram, highwater)
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
//
// limit caps the total number of records emitted to the client across both
// sources. Without it, when local and remote cover non-overlapping time
// ranges (typical of hot/warm vault topologies), each iter would emit
// q.Limit records and the merge would concatenate 2*q.Limit records with a
// temporal gap at the boundary. limit=0 means unbounded.
//
// lastLocalRec, when non-nil, captures the last record emitted from the
// local source. The local engine's own getToken tracks lastRefs that is
// updated BEFORE yield is called, so a sorted merge that "pulls ahead"
// by one record (necessary to compare TS across sources) leaves the
// iter's lastRefs one position past what the merge actually emitted to
// the client. The caller uses lastLocalRec to override the engine's
// over-advanced positions in the resume token.
func mergeIterators(
	ctx context.Context,
	sb *streamBatcher,
	localIter, remoteIter iter.Seq2[chunk.Record, error],
	orderBy query.OrderBy,
	reverse bool,
	limit int,
	transform *query.RecordTransform,
	streamedHistogram *streamedHistogramBuilder,
	highwater *time.Time,
	lastLocalRec *chunk.Record,
) error {
	isBefore := func(a, b time.Time) bool {
		if reverse {
			return a.After(b)
		}
		return a.Before(b)
	}

	// Pull synchronously from each iterator using iter.Pull2. This is
	// critical for resume-token correctness: the previous goroutine-pumped
	// channel design ran the iterator one record AHEAD of merge consumption
	// (the buffered channel holding a record that hadn't been emitted yet),
	// which advanced the local iter's internal lastRefs past records the
	// merge had not yet displayed. The next page then resumed from the
	// over-advanced position, silently skipping records. Pull2 ensures
	// each yield happens only when the merge actually pulls.
	localNext, localStop := iter.Pull2(localIter)
	defer localStop()
	remoteNext, remoteStop := iter.Pull2(remoteIter)
	defer remoteStop()

	type pending struct {
		rec chunk.Record
		err error
		ok  bool
	}
	pull := func(next func() (chunk.Record, error, bool)) *pending {
		rec, err, ok := next()
		if !ok {
			return nil
		}
		return &pending{rec: rec, err: err}
	}

	localPending := pull(localNext)
	remotePending := pull(remoteNext)

	emitted := 0
	for localPending != nil || remotePending != nil {
		var rec chunk.Record
		if localPending != nil && localPending.err != nil {
			return mapSearchError(localPending.err)
		}
		if remotePending != nil && remotePending.err != nil {
			return mapSearchError(remotePending.err)
		}

		var fromLocal bool
		switch {
		case localPending == nil:
			rec = remotePending.rec
			remotePending = pull(remoteNext)
		case remotePending == nil:
			rec = localPending.rec
			fromLocal = true
			localPending = pull(localNext)
		default:
			localTS := orderBy.RecordTS(localPending.rec)
			remoteTS := orderBy.RecordTS(remotePending.rec)
			if isBefore(localTS, remoteTS) {
				rec = localPending.rec
				fromLocal = true
				localPending = pull(localNext)
			} else {
				rec = remotePending.rec
				remotePending = pull(remoteNext)
			}
		}

		done, err := emitRecord(ctx, sb, rec, transform, streamedHistogram, highwater)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if fromLocal && lastLocalRec != nil {
			*lastLocalRec = rec
		}
		emitted++
		if limit > 0 && emitted >= limit {
			return nil
		}
	}
	return nil
}

// emitRecord applies an optional transform to a record and writes it to the
// batcher. Returns (done, err) where done=true means the transform is exhausted.
//
// When highwater is non-nil, every successfully-emitted record's IngestTS is
// recorded into *highwater. The merge stream emits monotonically (descending
// for reverse=true, ascending for forward), so the final value is the
// boundary the client should resume after on the next page — even when the
// emitted record came from a remote iterator (where the local engine's own
// highwater is unaware of the merge).
func emitRecord(ctx context.Context, sb *streamBatcher, rec chunk.Record, transform *query.RecordTransform, streamedHistogram *streamedHistogramBuilder, highwater *time.Time) (bool, error) {
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
		if highwater != nil {
			*highwater = rec.IngestTS
		}
		return transform.Done(), nil
	}
	if err := sb.add(recordToProto(rec)); err != nil {
		return false, err
	}
	if highwater != nil {
		*highwater = rec.IngestTS
	}
	return false, nil
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

// resolveUnboundedQuery fills in zero q.Start / q.End from the replicated
// vault-ctl Raft FSM so all nodes (coordinator + remotes) bucket histograms
// on the same grid.
//
// Each node's ComputeHistogram independently calls deriveTimeRange when q.Start
// or q.End is zero, walking its LOCAL chunk view to pick min(IngestStart) and
// max(IngestEnd). With remotes seeing different chunk sets, the resulting
// bucket grids don't align and mergeHistogramBuckets — which matches by
// exact TimestampMs — emits a fragmented histogram split across unrelated
// time ranges. Resolving here, before q.String() is forwarded, makes the
// remotes parse concrete start=/end= directives and skip deriveTimeRange.
//
// Bounded queries (last=5m, explicit start=/end=) are no-ops.
//
// Reads from VaultManifestEntriesFromCtlFSM, which goes directly through the
// vault-ctl Raft group's FSM rather than per-tier-instance state. Every node
// is a voter of every vault-ctl group (gastrolog-292yi), so the FSM is
// authoritative cluster-wide regardless of which node hosts the tier — a
// coordinator that runs no tier replicas still sees the full sealed manifest.
// Falls back to ListChunkMetas for the legacy memory-mode path (no GroupManager,
// no FSM); that path also picks up the active chunk for vaults that have not
// yet sealed any data.
func (s *QueryServer) resolveUnboundedQuery(ctx context.Context, q query.Query) query.Query {
	if !q.Start.IsZero() && !q.End.IsZero() {
		return q
	}
	selectedVaults := s.selectedOrAllVaults(ctx, q)
	earliest, latest := s.aggregateVaultBounds(selectedVaults)
	if earliest.IsZero() {
		// No visible chunks anywhere. Leave bounds zero so per-node
		// derivation runs — bucketing may still be misaligned in that
		// degenerate setup, but we have no better anchor.
		return q
	}
	// Bump latest to coordinator-now so the active chunk's tail is captured
	// even when its IngestEnd lags real time.
	if now := time.Now(); latest.Before(now) {
		latest = now
	}
	if q.Start.IsZero() {
		q.Start = earliest
	}
	if q.End.IsZero() {
		q.End = latest
	}
	return q
}

// selectedOrAllVaults returns vaults from the query's vault_id= filter, or
// every known vault when the filter is absent. Used by resolveUnboundedQuery
// to know which vaults' chunks contribute to the unbounded-query bound
// derivation.
func (s *QueryServer) selectedOrAllVaults(ctx context.Context, q query.Query) []glid.GLID {
	selected, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	if len(selected) > 0 || s.cfgStore == nil {
		return selected
	}
	vaults, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil
	}
	out := make([]glid.GLID, 0, len(vaults))
	for _, v := range vaults {
		out = append(out, v.ID)
	}
	return out
}

// aggregateVaultBounds returns (min IngestStart, max IngestEnd) across every
// chunk visible for the given vaults. Walks both the cluster-replicated
// vault-ctl FSM (sealed manifest, visible on every voter) and the local
// chunk manager (active + memory-mode tiers). Either source contributing
// nothing is fine — the function only collapses to (zero, zero) when neither
// has anything to say.
func (s *QueryServer) aggregateVaultBounds(vaults []glid.GLID) (time.Time, time.Time) {
	var earliest, latest time.Time
	track := func(start, end time.Time) {
		if !start.IsZero() && (earliest.IsZero() || start.Before(earliest)) {
			earliest = start
		}
		if !end.IsZero() && (latest.IsZero() || end.After(latest)) {
			latest = end
		}
	}
	for _, vid := range vaults {
		for _, e := range s.orch.VaultManifestEntriesFromCtlFSM(vid) {
			if e.RecordCount == 0 {
				continue
			}
			track(e.IngestStart, e.IngestEnd)
		}
		metas, err := s.orch.ListChunkMetas(vid)
		if err != nil {
			continue
		}
		for _, m := range metas {
			if m.RecordCount == 0 {
				continue
			}
			track(m.IngestStart, m.IngestEnd)
		}
	}
	return earliest, latest
}

// histogramFullyLocal returns true when this node is the leader of every
// queried vault. When true, the histogram can be computed entirely from
// local chunks without any cross-node fan-out. Follower replicas are NOT
// sufficient: the active (un-sealed) chunk lives only on the leader and
// is never replicated, so a follower-only view drops every record currently
// in the active chunk and produces an empty right edge on the histogram
// (last bars cut off at the last-sealed-chunk boundary instead of running
// up to "now"). Falls back conservatively to false on any config store
// error or when this node holds no leader vaults. See gastrolog-2g334
// (regression of the gastrolog-66b7x optimization).
func (s *QueryServer) histogramFullyLocal(ctx context.Context, q query.Query) bool {
	if s.cfgStore == nil {
		return false
	}
	localLeaders := s.orch.LocalLeaderVaultIDs()
	if len(localLeaders) == 0 {
		return false
	}
	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	if len(selectedVaults) == 0 {
		// No vault filter — consider every vault we know about.
		vaults, err := s.cfgStore.ListVaults(ctx)
		if err != nil {
			return false
		}
		for _, v := range vaults {
			selectedVaults = append(selectedVaults, v.ID)
		}
	}
	for _, vid := range selectedVaults {
		if !localLeaders[vid] {
			return false
		}
	}
	return true
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

	stats := dedupHistogramStats{
		histTotal:    builder.totalCount(),
		uniqueCount:  builder.uniqueCount,
		dedupedCount: builder.dedupedCount,
	}

	return builder.toProto(), stats, nil
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
		h.groupCounts[idx][safeutf8.String(lvl)]++
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
//
// HasMore is intentionally absent from the per-batch responses: only the
// final response (after the iterator drains) knows whether a resume token
// was produced, so only the final response can answer "are there more
// pages?" Setting HasMore=true on intermediate batches is a lie — there's
// no pagination state available mid-stream — and breaks any consumer that
// reads the field per message instead of overwriting on the last one.
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
		if err := b.stream.Send(&apiv1.SearchResponse{Records: b.batch}); err != nil {
			return err
		}
		b.batch = b.batch[:0]
	}
	return nil
}

func (b *streamBatcher) pending() []*apiv1.Record { return b.batch }
