package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
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
func (s *QueryServer) Search(
	ctx context.Context,
	req *connect.Request[apiv1.SearchRequest],
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	store := req.Msg.Store
	if store == "" {
		store = "default"
	}

	eng := s.orch.QueryEngine(store)
	if eng == nil {
		return connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	q := protoToQuery(req.Msg.Query)

	// Parse resume token from request
	var resume *query.ResumeToken
	if len(req.Msg.ResumeToken) > 0 {
		resume = protoToResumeToken(req.Msg.ResumeToken)
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
func (s *QueryServer) Follow(
	ctx context.Context,
	req *connect.Request[apiv1.FollowRequest],
	stream *connect.ServerStream[apiv1.FollowResponse],
) error {
	store := req.Msg.Store
	if store == "" {
		store = "default"
	}

	eng := s.orch.QueryEngine(store)
	if eng == nil {
		return connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	q := protoToQuery(req.Msg.Query)

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
func (s *QueryServer) Explain(
	ctx context.Context,
	req *connect.Request[apiv1.ExplainRequest],
) (*connect.Response[apiv1.ExplainResponse], error) {
	store := req.Msg.Store
	if store == "" {
		store = "default"
	}

	eng := s.orch.QueryEngine(store)
	if eng == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

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
// Resume token format: 16-byte chunk ID + 8-byte position (little-endian).
func protoToResumeToken(data []byte) *query.ResumeToken {
	if len(data) != 24 {
		return nil
	}
	var chunkID chunk.ChunkID
	copy(chunkID[:], data[:16])
	pos := uint64(data[16]) | uint64(data[17])<<8 | uint64(data[18])<<16 | uint64(data[19])<<24 |
		uint64(data[20])<<32 | uint64(data[21])<<40 | uint64(data[22])<<48 | uint64(data[23])<<56
	return &query.ResumeToken{
		Next: chunk.RecordRef{
			ChunkID: chunkID,
			Pos:     pos,
		},
	}
}

// resumeTokenToProto converts an internal resume token to proto bytes.
// Resume token format: 16-byte chunk ID + 8-byte position (little-endian).
func resumeTokenToProto(token *query.ResumeToken) []byte {
	if token == nil {
		return nil
	}
	data := make([]byte, 24)
	copy(data[:16], token.Next.ChunkID[:])
	pos := token.Next.Pos
	data[16] = byte(pos)
	data[17] = byte(pos >> 8)
	data[18] = byte(pos >> 16)
	data[19] = byte(pos >> 24)
	data[20] = byte(pos >> 32)
	data[21] = byte(pos >> 40)
	data[22] = byte(pos >> 48)
	data[23] = byte(pos >> 56)
	return data
}
