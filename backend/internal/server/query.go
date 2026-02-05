package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
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

	q := protoToQuery(req.Msg.Query)

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

	// Send remaining records with HasMore=false
	if err := stream.Send(&apiv1.SearchResponse{
		Records:     batch,
		ResumeToken: tokenBytes,
		HasMore:     false,
	}); err != nil {
		return err
	}

	return nil
}

// Follow executes a query and streams matching records, continuing with new arrivals.
// Follow requires a single-store query. If no store is specified, it defaults to "default".
func (s *QueryServer) Follow(
	ctx context.Context,
	req *connect.Request[apiv1.FollowRequest],
	stream *connect.ServerStream[apiv1.FollowResponse],
) error {
	q := protoToQuery(req.Msg.Query)

	// Extract store from query, or default to "default"
	allStores := s.orch.ListStores()
	stores, remainingExpr := query.ExtractStoreFilter(q.BoolExpr, allStores)

	var storeID string
	if len(stores) == 0 {
		storeID = "default"
	} else if len(stores) == 1 {
		storeID = stores[0]
	} else {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("follow requires a single store (use store=X)"))
	}

	eng := s.orch.QueryEngine(storeID)
	if eng == nil {
		return connect.NewError(connect.CodeNotFound, errors.New("store not found: "+storeID))
	}

	// Update query to remove store predicate
	q.BoolExpr = remainingExpr

	// Follow doesn't support resume tokens - it streams indefinitely
	iter, _ := eng.SearchThenFollow(ctx, q, nil)

	const batchSize = 100
	batch := make([]*apiv1.Record, 0, batchSize)

	for rec, err := range iter {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil // Normal termination for follow
			}
			return connect.NewError(connect.CodeInternal, err)
		}

		batch = append(batch, recordToProto(rec))

		if len(batch) >= batchSize {
			if err := stream.Send(&apiv1.FollowResponse{Records: batch}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Send remaining records (stream ended, e.g., due to limit)
	if len(batch) > 0 {
		if err := stream.Send(&apiv1.FollowResponse{Records: batch}); err != nil {
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

	q := protoToQuery(req.Msg.Query)

	plan, err := eng.Explain(ctx, q)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.ExplainResponse{
		Chunks: make([]*apiv1.ChunkPlan, 0, len(plan.ChunkPlans)),
	}

	for _, cp := range plan.ChunkPlans {
		chunkPlan := &apiv1.ChunkPlan{
			ChunkId:          cp.ChunkID[:],
			Sealed:           cp.Sealed,
			RecordCount:      int64(cp.RecordCount),
			ScanMode:         cp.ScanMode,
			EstimatedRecords: int64(cp.EstimatedScan),
			RuntimeFilters:   []string{cp.RuntimeFilter},
			Steps:            make([]*apiv1.PipelineStep, 0, len(cp.Pipeline)),
		}

		for _, step := range cp.Pipeline {
			chunkPlan.Steps = append(chunkPlan.Steps, &apiv1.PipelineStep{
				Name:           step.Index,
				InputEstimate:  int64(step.PositionsBefore),
				OutputEstimate: int64(step.PositionsAfter),
				Action:         step.Action,
				Reason:         step.Reason,
				Detail:         step.Details,
			})
		}

		resp.Chunks = append(resp.Chunks, chunkPlan)
	}

	return connect.NewResponse(resp), nil
}

// protoToQuery converts a proto Query to the internal query.Query type.
func protoToQuery(pq *apiv1.Query) query.Query {
	if pq == nil {
		return query.Query{}
	}

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

	// TODO: handle KV predicates

	return q
}

// recordToProto converts an internal Record to the proto type.
func recordToProto(rec chunk.Record) *apiv1.Record {
	return &apiv1.Record{
		IngestTs: timestamppb.New(rec.IngestTS),
		WriteTs:  timestamppb.New(rec.WriteTS),
		Attrs:    rec.Attrs,
		Raw:      rec.Raw,
		// TODO: add RecordRef
	}
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
		var chunkID chunk.ChunkID
		copy(chunkID[:], pos.ChunkId)
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
			ChunkId:  pos.ChunkID[:],
			Position: pos.Position,
		}
	}

	data, err := proto.Marshal(protoToken)
	if err != nil {
		return nil // Should not happen with valid data
	}
	return data
}
