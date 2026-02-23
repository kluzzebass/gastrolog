package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// QueryServer implements the QueryService.
type QueryServer struct {
	orch              *orchestrator.Orchestrator
	queryTimeout      time.Duration
	maxFollowDuration time.Duration // 0 = no limit
	maxResultCount    int64         // 0 = unlimited
}

var _ gastrologv1connect.QueryServiceHandler = (*QueryServer)(nil)

// NewQueryServer creates a new QueryServer.
func NewQueryServer(orch *orchestrator.Orchestrator, queryTimeout, maxFollowDuration time.Duration, maxResultCount int64) *QueryServer {
	return &QueryServer{orch: orch, queryTimeout: queryTimeout, maxFollowDuration: maxFollowDuration, maxResultCount: maxResultCount}
}

// Search executes a query and streams matching records.
// Searches across all stores; use store=X in query expression to filter.
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

	eng := s.orch.MultiStoreQueryEngine()

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Pipeline query: run and return table or records.
	if pipeline != nil && len(pipeline.Pipes) > 0 {
		result, err := eng.RunPipeline(ctx, q, pipeline)
		if err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
		if result.Table != nil {
			return stream.Send(&apiv1.SearchResponse{
				TableResult: tableResultToProto(result.Table, pipeline),
			})
		}
		// Non-aggregating pipeline: stream records.
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
		return stream.Send(&apiv1.SearchResponse{Records: batch})
	}

	// Clamp query limit to server-configured max.
	if s.maxResultCount > 0 && (q.Limit == 0 || int64(q.Limit) > s.maxResultCount) {
		q.Limit = int(s.maxResultCount)
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
			if errors.Is(err, context.DeadlineExceeded) {
				return connect.NewError(connect.CodeDeadlineExceeded, err)
			}
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
	if s.maxFollowDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.maxFollowDuration)
		defer cancel()
	}

	eng := s.orch.MultiStoreQueryEngine()

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
					fmt.Errorf("stats operator is not supported in follow mode"))
			case *querylang.SortOp:
				return connect.NewError(connect.CodeInvalidArgument,
					fmt.Errorf("sort operator is not supported in follow mode"))
			case *querylang.TailOp:
				return connect.NewError(connect.CodeInvalidArgument,
					fmt.Errorf("tail operator is not supported in follow mode"))
			case *querylang.SliceOp:
				return connect.NewError(connect.CodeInvalidArgument,
					fmt.Errorf("slice operator is not supported in follow mode"))
			case *querylang.TimechartOp:
				return connect.NewError(connect.CodeInvalidArgument,
					fmt.Errorf("timechart operator is not supported in follow mode"))
			}
		}
	}

	// Use Follow which continuously polls for new records.
	iter := eng.Follow(ctx, q)

	// Send each record immediately for real-time tailing.
	for rec, err := range iter {
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
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

	q, _, err := protoToQuery(req.Msg.Query)
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
	resp.Expression = plan.Query.String()
	if !plan.Query.Start.IsZero() {
		resp.QueryStart = timestamppb.New(plan.Query.Start)
	}
	if !plan.Query.End.IsZero() {
		resp.QueryEnd = timestamppb.New(plan.Query.End)
	}

	for _, cp := range plan.ChunkPlans {
		chunkPlan := &apiv1.ChunkPlan{
			StoreId:          cp.StoreID.String(),
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

// GetContext returns records surrounding a specific record, across all stores.
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
	if ref == nil || ref.StoreId == "" || ref.ChunkId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("ref must include store_id, chunk_id, and pos"))
	}

	storeID, connErr := parseUUID(ref.StoreId)
	if connErr != nil {
		return nil, connErr
	}

	chunkID, err := chunk.ParseChunkID(ref.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid chunk_id: %w", err))
	}

	eng := s.orch.MultiStoreQueryEngine()
	result, err := eng.GetContext(ctx, query.ContextRef{
		StoreID: storeID,
		ChunkID: chunkID,
		Pos:     ref.Pos,
	}, int(req.Msg.Before), int(req.Msg.After))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Build response.
	resp := &apiv1.GetContextResponse{
		Anchor: recordToProto(result.Anchor),
		Before: make([]*apiv1.Record, 0, len(result.Before)),
		After:  make([]*apiv1.Record, 0, len(result.After)),
	}
	for _, rec := range result.Before {
		resp.Before = append(resp.Before, recordToProto(rec))
	}
	for _, rec := range result.After {
		resp.After = append(resp.After, recordToProto(rec))
	}

	return connect.NewResponse(resp), nil
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
		PipeKeywords:  []string{"stats", "where", "eval", "sort", "head", "tail", "slice", "rename", "fields", "timechart", "raw"},
		PipeFunctions: funcs,
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
// is parsed through the pipeline parser. If the expression contains pipe
// operators (e.g. "| stats count"), the pipeline is returned; otherwise
// only the filter expression is set on the query.
func parseExpression(expr string) (query.Query, *querylang.Pipeline, error) {
	if len(expr) > maxExpressionLength {
		return query.Query{}, nil, fmt.Errorf("expression too long: %d bytes (max %d)", len(expr), maxExpressionLength)
	}
	parts := strings.Fields(expr)
	if len(parts) == 0 {
		return query.Query{}, nil, nil
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
					return q, nil, fmt.Errorf("invalid start time: %w", err)
				}
				q.Start = t
				continue
			case "end":
				t, err := parseTime(v)
				if err != nil {
					return q, nil, fmt.Errorf("invalid end time: %w", err)
				}
				q.End = t
				continue
			case "last":
				d, err := parseDuration(v)
				if err != nil {
					return q, nil, fmt.Errorf("invalid last duration: %w", err)
				}
				now := time.Now()
				q.Start = now.Add(-d)
				q.End = now
				continue
			case "source_start":
				t, err := parseTime(v)
				if err != nil {
					return q, nil, fmt.Errorf("invalid source_start time: %w", err)
				}
				q.SourceStart = t
				continue
			case "source_end":
				t, err := parseTime(v)
				if err != nil {
					return q, nil, fmt.Errorf("invalid source_end time: %w", err)
				}
				q.SourceEnd = t
				continue
			case "ingest_start":
				t, err := parseTime(v)
				if err != nil {
					return q, nil, fmt.Errorf("invalid ingest_start time: %w", err)
				}
				q.IngestStart = t
				continue
			case "ingest_end":
				t, err := parseTime(v)
				if err != nil {
					return q, nil, fmt.Errorf("invalid ingest_end time: %w", err)
				}
				q.IngestEnd = t
				continue
			case "limit":
				var n int
				if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
					return q, nil, fmt.Errorf("invalid limit: %w", err)
				}
				q.Limit = n
				continue
			case "pos":
				var n uint64
				if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
					return q, nil, fmt.Errorf("invalid pos: %w", err)
				}
				q.Pos = &n
				continue
			}
		}
		filterParts = append(filterParts, part)
	}

	if len(filterParts) > 0 {
		pipeline, err := querylang.ParsePipeline(strings.Join(filterParts, " "))
		if err != nil {
			return q, nil, fmt.Errorf("parse error: %w", err)
		}
		q.BoolExpr = pipeline.Filter
		if len(pipeline.Pipes) > 0 {
			return q, pipeline, nil
		}
	}

	return q, nil, nil
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
	}
	if hasRaw {
		resultType = "raw"
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
		IngestTs: timestamppb.New(rec.IngestTS),
		WriteTs:  timestamppb.New(rec.WriteTS),
		Attrs:    rec.Attrs,
		Raw:      rec.Raw,
		Ref: &apiv1.RecordRef{
			ChunkId: rec.Ref.ChunkID.String(),
			Pos:     rec.Ref.Pos,
			StoreId: rec.StoreID.String(),
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
		storeID, err := uuid.Parse(pos.StoreId)
		if err != nil {
			return nil, fmt.Errorf("invalid store ID in resume token: %w", err)
		}
		token.Positions[i] = query.MultiStorePosition{
			StoreID:  storeID,
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
			StoreId:  pos.StoreID.String(),
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
