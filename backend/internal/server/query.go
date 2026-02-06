package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// QueryServer implements the QueryService.
type QueryServer struct {
	orch *orchestrator.Orchestrator
}

var _ gastrologv1connect.QueryServiceHandler = (*QueryServer)(nil)

// NewQueryServer creates a new QueryServer.
func NewQueryServer(orch *orchestrator.Orchestrator) *QueryServer {
	return &QueryServer{orch: orch}
}

// Search executes a query and streams matching records.
// Searches across all stores; use store=X in query expression to filter.
func (s *QueryServer) Search(
	ctx context.Context,
	req *connect.Request[apiv1.SearchRequest],
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	eng := s.orch.MultiStoreQueryEngine()

	q, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Parse resume token from request
	var resume *query.ResumeToken
	if len(req.Msg.ResumeToken) > 0 {
		var err error
		resume, err = protoToResumeToken(req.Msg.ResumeToken)
		if err != nil {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	iter, getToken := eng.Search(ctx, q, resume)

	// Batch records for efficiency
	const batchSize = 100
	batch := make([]*apiv1.Record, 0, batchSize)

	for rec, err := range iter {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return connect.NewError(connect.CodeCanceled, err)
			}
			if errors.Is(err, query.ErrInvalidResumeToken) {
				return connect.NewError(connect.CodeInvalidArgument, err)
			}
			return connect.NewError(connect.CodeInternal, err)
		}

		batch = append(batch, recordToProto(rec))

		if len(batch) >= batchSize {
			if err := stream.Send(&apiv1.SearchResponse{Records: batch, HasMore: true}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Get resume token for final response
	var tokenBytes []byte
	if token := getToken(); token != nil {
		tokenBytes = resumeTokenToProto(token)
	}

	// Send remaining records. HasMore=true when a resume token exists
	// (limit was reached and more records are available).
	if err := stream.Send(&apiv1.SearchResponse{
		Records:     batch,
		ResumeToken: tokenBytes,
		HasMore:     len(tokenBytes) > 0,
	}); err != nil {
		return err
	}

	return nil
}

// Follow executes a query and streams matching records, continuously polling for new arrivals.
// This is a tail -f style operation that never completes until the client disconnects.
func (s *QueryServer) Follow(
	ctx context.Context,
	req *connect.Request[apiv1.FollowRequest],
	stream *connect.ServerStream[apiv1.FollowResponse],
) error {
	eng := s.orch.MultiStoreQueryEngine()

	q, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Use Follow which continuously polls for new records.
	iter := eng.Follow(ctx, q)

	// Send each record immediately for real-time tailing.
	for rec, err := range iter {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil // Normal termination for follow
			}
			return connect.NewError(connect.CodeInternal, err)
		}

		if err := stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{recordToProto(rec)}}); err != nil {
			return err
		}
	}

	return nil
}

// Explain returns the query execution plan without executing.
// Explains the plan for all stores; use store=X in query expression to filter.
func (s *QueryServer) Explain(
	ctx context.Context,
	req *connect.Request[apiv1.ExplainRequest],
) (*connect.Response[apiv1.ExplainResponse], error) {
	eng := s.orch.MultiStoreQueryEngine()

	q, err := protoToQuery(req.Msg.Query)
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
		TotalChunks: int32(plan.TotalChunks),
	}
	if plan.Query.BoolExpr != nil {
		resp.Expression = plan.Query.BoolExpr.String()
	}

	for _, cp := range plan.ChunkPlans {
		chunkPlan := &apiv1.ChunkPlan{
			StoreId:          cp.StoreID,
			ChunkId:          cp.ChunkID.String(),
			Sealed:           cp.Sealed,
			RecordCount:      int64(cp.RecordCount),
			ScanMode:         cp.ScanMode,
			EstimatedRecords: int64(cp.EstimatedScan),
			RuntimeFilters:   []string{cp.RuntimeFilter},
			Steps:            pipelineStepsToProto(cp.Pipeline),
			SkipReason:       cp.SkipReason,
		}
		if !cp.StartTS.IsZero() {
			chunkPlan.StartTs = timestamppb.New(cp.StartTS)
		}
		if !cp.EndTS.IsZero() {
			chunkPlan.EndTs = timestamppb.New(cp.EndTS)
		}

		for _, bp := range cp.BranchPlans {
			chunkPlan.BranchPlans = append(chunkPlan.BranchPlans, &apiv1.BranchPlan{
				Expression:       bp.BranchExpr,
				Steps:            pipelineStepsToProto(bp.Pipeline),
				Skipped:          bp.Skipped,
				SkipReason:       bp.SkipReason,
				EstimatedRecords: int64(bp.EstimatedScan),
			})
		}

		resp.Chunks = append(resp.Chunks, chunkPlan)
	}

	return connect.NewResponse(resp), nil
}

// Histogram returns record counts bucketed by time.
//
// Two modes:
//   - Unfiltered (no tokens, kv, severity): uses FindStartPosition binary search
//     on idx.log for O(buckets * log(n)) per chunk. Very fast.
//   - Filtered: runs the full query engine search (unlimited) and buckets each
//     matching record. Capped at 1M records to prevent runaway scans.
func (s *QueryServer) Histogram(
	ctx context.Context,
	req *connect.Request[apiv1.HistogramRequest],
) (*connect.Response[apiv1.HistogramResponse], error) {
	numBuckets := int(req.Msg.Buckets)
	if numBuckets <= 0 {
		numBuckets = 50
	}
	if numBuckets > 500 {
		numBuckets = 500
	}

	// Parse expression to extract time bounds, store filter, and query filters.
	var q query.Query
	if req.Msg.Expression != "" {
		var err error
		q, err = parseExpression(req.Msg.Expression)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	if req.Msg.Start != nil {
		q.Start = req.Msg.Start.AsTime()
	}
	if req.Msg.End != nil {
		q.End = req.Msg.End.AsTime()
	}

	// Determine which stores to query.
	allStores := s.orch.ListStores()
	selectedStores := allStores
	if q.BoolExpr != nil {
		stores, _ := query.ExtractStoreFilter(q.BoolExpr, allStores)
		if stores != nil {
			selectedStores = stores
		}
	}

	// If no time range, derive from chunk metadata.
	if q.Start.IsZero() || q.End.IsZero() {
		for _, storeID := range selectedStores {
			cm := s.orch.ChunkManager(storeID)
			if cm == nil {
				continue
			}
			metas, err := cm.List()
			if err != nil {
				continue
			}
			for _, meta := range metas {
				if meta.RecordCount == 0 {
					continue
				}
				if q.Start.IsZero() || meta.StartTS.Before(q.Start) {
					q.Start = meta.StartTS
				}
				if q.End.IsZero() || meta.EndTS.After(q.End) {
					q.End = meta.EndTS
				}
			}
		}
	}

	// Normalize: histogram always needs lower < upper regardless of query direction.
	start, end := q.Start, q.End
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return connect.NewResponse(&apiv1.HistogramResponse{}), nil
	}

	bucketWidth := end.Sub(start) / time.Duration(numBuckets)
	if bucketWidth <= 0 {
		bucketWidth = time.Second
	}

	counts := make([]int64, numBuckets)
	levelCounts := make([]map[string]int64, numBuckets)
	for i := range levelCounts {
		levelCounts[i] = make(map[string]int64)
	}
	hasFilter := q.BoolExpr != nil

	if hasFilter {
		// Filtered path: scan matching records and bucket them.
		q.Limit = 0
		eng := s.orch.MultiStoreQueryEngine()
		iter, _ := eng.Search(ctx, q, nil)
		const maxScan = 1_000_000
		scanned := 0
		for rec, err := range iter {
			if err != nil {
				if errors.Is(err, context.Canceled) {
					break
				}
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			ts := rec.WriteTS
			if ts.Before(start) || !ts.Before(end) {
				continue
			}
			idx := int(ts.Sub(start) / bucketWidth)
			if idx >= numBuckets {
				idx = numBuckets - 1
			}
			counts[idx]++
			scanned++
			if scanned >= maxScan {
				break
			}
		}
	} else {
		// Unfiltered path: use FindStartPosition for O(buckets * log(n)).
		for _, storeID := range selectedStores {
			cm := s.orch.ChunkManager(storeID)
			if cm == nil {
				continue
			}
			im := s.orch.IndexManager(storeID)
			metas, err := cm.List()
			if err != nil {
				continue
			}
			for _, meta := range metas {
				if meta.RecordCount == 0 {
					continue
				}
				if meta.EndTS.Before(start) || !meta.StartTS.Before(end) {
					continue
				}
				s.histogramChunkFast(cm, meta, start, bucketWidth, numBuckets, counts)
				if meta.Sealed {
					s.histogramChunkSeverity(cm, im, meta, start, bucketWidth, numBuckets, levelCounts)
				}
			}
		}
	}

	resp := &apiv1.HistogramResponse{
		Start:   timestamppb.New(start),
		End:     timestamppb.New(end),
		Buckets: make([]*apiv1.HistogramBucket, numBuckets),
	}
	for i := 0; i < numBuckets; i++ {
		bucket := &apiv1.HistogramBucket{
			Ts:    timestamppb.New(start.Add(bucketWidth * time.Duration(i))),
			Count: counts[i],
		}
		if len(levelCounts[i]) > 0 {
			bucket.LevelCounts = levelCounts[i]
		}
		resp.Buckets[i] = bucket
	}

	return connect.NewResponse(resp), nil
}

// histogramChunkFast counts records per bucket using binary search on idx.log.
// O(buckets * log(n)) per chunk, no record scanning.
func (s *QueryServer) histogramChunkFast(
	cm chunk.ChunkManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	numBuckets int,
	counts []int64,
) {
	end := start.Add(bucketWidth * time.Duration(numBuckets))

	firstBucket := 0
	if meta.StartTS.After(start) {
		firstBucket = int(meta.StartTS.Sub(start) / bucketWidth)
		if firstBucket >= numBuckets {
			return
		}
	}
	lastBucket := numBuckets - 1
	if meta.EndTS.Before(end) {
		lastBucket = int(meta.EndTS.Sub(start) / bucketWidth)
		if lastBucket >= numBuckets {
			lastBucket = numBuckets - 1
		}
	}

	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))

		var startPos uint64
		if pos, found, err := cm.FindStartPosition(meta.ID, bStart); err == nil && found {
			startPos = pos
		}

		var endPos uint64
		if !bEnd.Before(meta.EndTS) {
			endPos = uint64(meta.RecordCount)
		} else if pos, found, err := cm.FindStartPosition(meta.ID, bEnd); err == nil && found {
			endPos = pos
		}

		if endPos > startPos {
			counts[b] += int64(endPos - startPos)
		}
	}
}

// severityLevels are the canonical severity names we track in histograms.
var severityLevels = []string{"error", "warn", "info", "debug", "trace"}

// severityKVLookups maps canonical severity names to the key=value pairs to look up
// in the KV index (extracted from message text, e.g. level=error).
var severityKVLookups = map[string][][2]string{
	"error": {{"level", "error"}, {"level", "err"}},
	"warn":  {{"level", "warn"}, {"level", "warning"}},
	"info":  {{"level", "info"}},
	"debug": {{"level", "debug"}},
	"trace": {{"level", "trace"}},
}

// severityAttrKVLookups maps canonical severity names to the key=value pairs to look up
// in the attr KV index (from record attributes, e.g. severity_name=error).
var severityAttrKVLookups = map[string][][2]string{
	"error": {{"severity_name", "err"}, {"severity_name", "error"}},
	"warn":  {{"severity_name", "warning"}, {"severity_name", "warn"}},
	"info":  {{"severity_name", "info"}},
	"debug": {{"severity_name", "debug"}},
	"trace": {{"severity_name", "trace"}},
}

// histogramChunkSeverity populates per-level counts for a chunk using KV indexes.
// Only works on sealed, indexed chunks. Returns false if indexes are unavailable.
func (s *QueryServer) histogramChunkSeverity(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	numBuckets int,
	levelCounts []map[string]int64,
) bool {
	if im == nil {
		return false
	}

	// Open KV index (from message text).
	kvIdx, _, err := im.OpenKVIndex(meta.ID)
	if err != nil {
		return false
	}
	kvReader := index.NewKVIndexReader(meta.ID, kvIdx.Entries())

	// Open attr KV index (from record attributes).
	attrKVIdx, err := im.OpenAttrKVIndex(meta.ID)
	var attrKVReader *index.KVIndexReader
	if err == nil {
		// AttrKVIndex uses AttrKVIndexEntry, but we need KVIndexReader.
		// Convert entries.
		attrEntries := attrKVIdx.Entries()
		kvEntries := make([]index.KVIndexEntry, len(attrEntries))
		for i, e := range attrEntries {
			kvEntries[i] = index.KVIndexEntry{Key: e.Key, Value: e.Value, Positions: e.Positions}
		}
		attrKVReader = index.NewKVIndexReader(meta.ID, kvEntries)
	}

	for _, level := range severityLevels {
		// Collect positions from both KV and attr KV indexes.
		var allPositions []uint64

		for _, kv := range severityKVLookups[level] {
			if positions, found := kvReader.Lookup(kv[0], kv[1]); found {
				allPositions = append(allPositions, positions...)
			}
		}

		if attrKVReader != nil {
			for _, kv := range severityAttrKVLookups[level] {
				if positions, found := attrKVReader.Lookup(kv[0], kv[1]); found {
					allPositions = append(allPositions, positions...)
				}
			}
		}

		if len(allPositions) == 0 {
			continue
		}

		// Deduplicate positions (a record might match both level=error and severity_name=error).
		slices.Sort(allPositions)
		allPositions = slices.Compact(allPositions)

		// Read WriteTS for each position and bucket them.
		timestamps, err := cm.ReadWriteTimestamps(meta.ID, allPositions)
		if err != nil {
			continue
		}

		for _, ts := range timestamps {
			if ts.Before(start) || !ts.Before(start.Add(bucketWidth*time.Duration(numBuckets))) {
				continue
			}
			idx := int(ts.Sub(start) / bucketWidth)
			if idx >= numBuckets {
				idx = numBuckets - 1
			}
			levelCounts[idx][level]++
		}
	}

	return true
}

// protoToQuery converts a proto Query to the internal query.Query type.
// If the Expression field is set, it is parsed server-side and takes
// precedence over the legacy Tokens/KvPredicates fields.
func protoToQuery(pq *apiv1.Query) (query.Query, error) {
	if pq == nil {
		return query.Query{}, nil
	}

	// If Expression is set, parse it server-side (same logic as repl/parse.go).
	// Proto-level fields (Limit, Start, End) override expression-level values
	// when set, so the frontend can control page size without injecting limit=
	// into the expression string.
	if pq.Expression != "" {
		q, err := parseExpression(pq.Expression)
		if err != nil {
			return q, err
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
		return q, nil
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

	return q, nil
}

// parseExpression parses a raw query expression string into a Query.
// Control arguments (start=, end=, limit=) are extracted; the remainder
// is parsed through the querylang parser into BoolExpr.
func parseExpression(expr string) (query.Query, error) {
	parts := strings.Fields(expr)
	if len(parts) == 0 {
		return query.Query{}, nil
	}

	var q query.Query
	var filterParts []string

	for _, part := range parts {
		k, v, ok := strings.Cut(part, "=")
		if ok {
			switch k {
			case "reverse":
				q.IsReverse = v == "true"
				continue
			case "start":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Errorf("invalid start time: %w", err)
				}
				q.Start = t
				continue
			case "end":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Errorf("invalid end time: %w", err)
				}
				q.End = t
				continue
			case "source_start":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Errorf("invalid source_start time: %w", err)
				}
				q.SourceStart = t
				continue
			case "source_end":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Errorf("invalid source_end time: %w", err)
				}
				q.SourceEnd = t
				continue
			case "ingest_start":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Errorf("invalid ingest_start time: %w", err)
				}
				q.IngestStart = t
				continue
			case "ingest_end":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Errorf("invalid ingest_end time: %w", err)
				}
				q.IngestEnd = t
				continue
			case "limit":
				var n int
				if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
					return q, fmt.Errorf("invalid limit: %w", err)
				}
				q.Limit = n
				continue
			case "pos":
				var n uint64
				if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
					return q, fmt.Errorf("invalid pos: %w", err)
				}
				q.Pos = &n
				continue
			}
		}
		filterParts = append(filterParts, part)
	}

	if len(filterParts) > 0 {
		parsed, err := querylang.Parse(strings.Join(filterParts, " "))
		if err != nil {
			return q, fmt.Errorf("parse error: %w", err)
		}
		q.BoolExpr = parsed
	}

	return q, nil
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
	if n, err := fmt.Sscanf(s, "%d", &unix); err == nil && n == 1 && fmt.Sprintf("%d", unix) == s {
		return time.Unix(unix, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %s (use RFC3339 or Unix timestamp)", s)
}

// pipelineStepsToProto converts internal PipelineSteps to proto.
func pipelineStepsToProto(steps []query.PipelineStep) []*apiv1.PipelineStep {
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

// recordToProto converts an internal Record to the proto type.
func recordToProto(rec chunk.Record) *apiv1.Record {
	r := &apiv1.Record{
		IngestTs: timestamppb.New(rec.IngestTS),
		WriteTs:  timestamppb.New(rec.WriteTS),
		Attrs:    rec.Attrs,
		Raw:      rec.Raw,
		Ref: &apiv1.RecordRef{
			ChunkId: rec.Ref.ChunkID.String(),
			Pos:     rec.Ref.Pos,
			StoreId: rec.StoreID,
		},
	}
	if !rec.SourceTS.IsZero() {
		r.SourceTs = timestamppb.New(rec.SourceTS)
	}
	return r
}

// protoToResumeToken converts a proto resume token to the internal type.
// Uses protobuf encoding for the ResumeToken message.
func protoToResumeToken(data []byte) (*query.ResumeToken, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Decode proto message
	var protoToken apiv1.ResumeToken
	if err := proto.Unmarshal(data, &protoToken); err != nil {
		return nil, err
	}

	// Convert to internal type
	token := &query.ResumeToken{
		Positions: make([]query.MultiStorePosition, len(protoToken.Positions)),
	}

	for i, pos := range protoToken.Positions {
		chunkID, err := chunk.ParseChunkID(pos.ChunkId)
		if err != nil {
			return nil, fmt.Errorf("invalid chunk ID in resume token: %w", err)
		}
		token.Positions[i] = query.MultiStorePosition{
			StoreID:  pos.StoreId,
			ChunkID:  chunkID,
			Position: pos.Position,
		}
	}

	return token, nil
}

// resumeTokenToProto converts an internal resume token to proto bytes.
// Uses protobuf encoding for the ResumeToken message.
func resumeTokenToProto(token *query.ResumeToken) []byte {
	if token == nil || len(token.Positions) == 0 {
		return nil
	}

	protoToken := &apiv1.ResumeToken{
		Positions: make([]*apiv1.StorePosition, len(token.Positions)),
	}

	for i, pos := range token.Positions {
		protoToken.Positions[i] = &apiv1.StorePosition{
			StoreId:  pos.StoreID,
			ChunkId:  pos.ChunkID.String(),
			Position: pos.Position,
		}
	}

	data, err := proto.Marshal(protoToken)
	if err != nil {
		return nil // Should not happen with valid data
	}
	return data
}
