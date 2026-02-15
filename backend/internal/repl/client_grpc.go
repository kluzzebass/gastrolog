package repl

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"net"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/query"
)

// GRPCClient implements Client by making gRPC calls to a remote server.
type GRPCClient struct {
	query     gastrologv1connect.QueryServiceClient
	store     gastrologv1connect.StoreServiceClient
	config    gastrologv1connect.ConfigServiceClient
	lifecycle gastrologv1connect.LifecycleServiceClient
}

var _ Client = (*GRPCClient)(nil)

// NewGRPCClient creates a client that connects to a remote server via TCP.
func NewGRPCClient(baseURL string) *GRPCClient {
	return &GRPCClient{
		query:     gastrologv1connect.NewQueryServiceClient(http.DefaultClient, baseURL),
		store:     gastrologv1connect.NewStoreServiceClient(http.DefaultClient, baseURL),
		config:    gastrologv1connect.NewConfigServiceClient(http.DefaultClient, baseURL),
		lifecycle: gastrologv1connect.NewLifecycleServiceClient(http.DefaultClient, baseURL),
	}
}

// NewGRPCClientUnix creates a client that connects to a remote server via Unix socket.
func NewGRPCClientUnix(socketPath string) *GRPCClient {
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
		},
	}
	// For Unix sockets, we use a dummy URL since the actual connection
	// is made via the custom dialer.
	return &GRPCClient{
		query:     gastrologv1connect.NewQueryServiceClient(httpClient, "http://localhost"),
		store:     gastrologv1connect.NewStoreServiceClient(httpClient, "http://localhost"),
		config:    gastrologv1connect.NewConfigServiceClient(httpClient, "http://localhost"),
		lifecycle: gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://localhost"),
	}
}

// NewGRPCClientWithHTTP creates a client with a custom HTTP client.
// This can be used for embedded mode with an in-memory transport.
func NewGRPCClientWithHTTP(httpClient connect.HTTPClient, baseURL string) *GRPCClient {
	return &GRPCClient{
		query:     gastrologv1connect.NewQueryServiceClient(httpClient, baseURL),
		store:     gastrologv1connect.NewStoreServiceClient(httpClient, baseURL),
		config:    gastrologv1connect.NewConfigServiceClient(httpClient, baseURL),
		lifecycle: gastrologv1connect.NewLifecycleServiceClient(httpClient, baseURL),
	}
}

func (c *GRPCClient) Search(ctx context.Context, store string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	// If a specific store is requested, add it as a query predicate.
	if store != "" && store != "default" {
		q = q.WithStorePredicate(store)
	}

	protoQuery := queryToProto(q)

	req := connect.NewRequest(&apiv1.SearchRequest{
		Query:       protoQuery,
		ResumeToken: resumeTokenToBytes(resume),
	})

	stream, err := c.query.Search(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	var lastToken []byte

	seq := func(yield func(chunk.Record, error) bool) {
		for stream.Receive() {
			msg := stream.Msg()
			lastToken = msg.ResumeToken
			for _, rec := range msg.Records {
				if !yield(protoToRecord(rec), nil) {
					return
				}
			}
		}
		if err := stream.Err(); err != nil && !errors.Is(err, io.EOF) {
			yield(chunk.Record{}, err)
		}
	}

	getToken := func() *query.ResumeToken {
		return bytesToResumeToken(lastToken)
	}

	return seq, getToken, nil
}

func (c *GRPCClient) Explain(ctx context.Context, store string, q query.Query) (*query.QueryPlan, error) {
	// If a specific store is requested, add it as a query predicate.
	if store != "" && store != "default" {
		q = q.WithStorePredicate(store)
	}

	protoQuery := queryToProto(q)

	req := connect.NewRequest(&apiv1.ExplainRequest{
		Query: protoQuery,
	})

	resp, err := c.query.Explain(ctx, req)
	if err != nil {
		return nil, err
	}

	return protoToQueryPlan(resp.Msg), nil
}

func (c *GRPCClient) Follow(ctx context.Context, store string, q query.Query) (iter.Seq2[chunk.Record, error], error) {
	// If a specific store is requested, add it as a query predicate.
	if store != "" && store != "default" {
		q = q.WithStorePredicate(store)
	}

	protoQuery := queryToProto(q)

	req := connect.NewRequest(&apiv1.FollowRequest{
		Query: protoQuery,
	})

	stream, err := c.query.Follow(ctx, req)
	if err != nil {
		return nil, err
	}

	seq := func(yield func(chunk.Record, error) bool) {
		for stream.Receive() {
			msg := stream.Msg()
			for _, rec := range msg.Records {
				if !yield(protoToRecord(rec), nil) {
					return
				}
			}
		}
		if err := stream.Err(); err != nil && !errors.Is(err, io.EOF) {
			yield(chunk.Record{}, err)
		}
	}

	return seq, nil
}

func (c *GRPCClient) ListStores() []StoreInfo {
	resp, err := c.store.ListStores(context.Background(), connect.NewRequest(&apiv1.ListStoresRequest{}))
	if err != nil {
		return nil
	}
	stores := make([]StoreInfo, len(resp.Msg.Stores))
	for i, s := range resp.Msg.Stores {
		stores[i] = StoreInfo{ID: s.Id, Name: s.Name}
	}
	return stores
}

func (c *GRPCClient) ChunkManager(store string) ChunkReader {
	return &grpcChunkReader{client: c, store: store}
}

func (c *GRPCClient) IndexManager(store string) IndexReader {
	return &grpcIndexReader{client: c, store: store}
}

func (c *GRPCClient) Analyzer(store string) AnalyzerClient {
	return &grpcAnalyzer{client: c, store: store}
}

func (c *GRPCClient) IsRunning() bool {
	resp, err := c.lifecycle.Health(context.Background(), connect.NewRequest(&apiv1.HealthRequest{}))
	if err != nil {
		return false
	}
	return resp.Msg.Status == apiv1.Status_STATUS_HEALTHY
}

// grpcChunkReader implements ChunkReader via gRPC.
type grpcChunkReader struct {
	client *GRPCClient
	store  string
}

func (r *grpcChunkReader) List() ([]chunk.ChunkMeta, error) {
	resp, err := r.client.store.ListChunks(context.Background(), connect.NewRequest(&apiv1.ListChunksRequest{
		Store: r.store,
	}))
	if err != nil {
		return nil, err
	}

	metas := make([]chunk.ChunkMeta, len(resp.Msg.Chunks))
	for i, c := range resp.Msg.Chunks {
		metas[i] = protoToChunkMeta(c)
	}
	return metas, nil
}

func (r *grpcChunkReader) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	resp, err := r.client.store.GetChunk(context.Background(), connect.NewRequest(&apiv1.GetChunkRequest{
		Store:   r.store,
		ChunkId: id.String(),
	}))
	if err != nil {
		return chunk.ChunkMeta{}, err
	}
	return protoToChunkMeta(resp.Msg.Chunk), nil
}

func (r *grpcChunkReader) Active() *chunk.ChunkMeta {
	// List chunks and find the unsealed one
	metas, err := r.List()
	if err != nil {
		return nil
	}
	for i := range metas {
		if !metas[i].Sealed {
			return &metas[i]
		}
	}
	return nil
}

func (r *grpcChunkReader) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	// Opening a cursor requires streaming records which isn't directly exposed
	return nil, errors.New("cursor not supported via gRPC client")
}

// grpcIndexReader implements IndexReader via gRPC.
type grpcIndexReader struct {
	client *GRPCClient
	store  string
}

func (r *grpcIndexReader) IndexesComplete(id chunk.ChunkID) (bool, error) {
	resp, err := r.client.store.GetIndexes(context.Background(), connect.NewRequest(&apiv1.GetIndexesRequest{
		Store:   r.store,
		ChunkId: id.String(),
	}))
	if err != nil {
		return false, err
	}
	// Check if all indexes exist
	for _, idx := range resp.Msg.Indexes {
		if !idx.Exists {
			return false, nil
		}
	}
	return true, nil
}

func (r *grpcIndexReader) OpenTokenIndex(id chunk.ChunkID) (TokenIndex, error) {
	// Opening an index reader isn't supported via gRPC
	return nil, errors.New("token index not supported via gRPC client")
}

// grpcAnalyzer implements AnalyzerClient via gRPC.
type grpcAnalyzer struct {
	client *GRPCClient
	store  string
}

func (a *grpcAnalyzer) AnalyzeChunk(id chunk.ChunkID) (*analyzer.ChunkAnalysis, error) {
	resp, err := a.client.store.AnalyzeChunk(context.Background(), connect.NewRequest(&apiv1.AnalyzeChunkRequest{
		Store:   a.store,
		ChunkId: id.String(),
	}))
	if err != nil {
		return nil, err
	}
	if len(resp.Msg.Analyses) == 0 {
		return nil, errors.New("no analysis returned")
	}
	return protoToChunkAnalysis(resp.Msg.Analyses[0]), nil
}

func (a *grpcAnalyzer) AnalyzeAll() (*analyzer.AggregateAnalysis, error) {
	resp, err := a.client.store.AnalyzeChunk(context.Background(), connect.NewRequest(&apiv1.AnalyzeChunkRequest{
		Store: a.store, // Empty ChunkId = analyze all
	}))
	if err != nil {
		return nil, err
	}
	return protoToAggregateAnalysis(resp.Msg.Analyses), nil
}

// Conversion helpers: internal -> proto

func queryToProto(q query.Query) *apiv1.Query {
	// Prefer sending the raw expression string (parsed server-side).
	// This preserves full boolean query semantics across the gRPC boundary.
	if q.RawExpression != "" {
		return &apiv1.Query{Expression: q.RawExpression}
	}

	// Legacy path: send structured fields.
	pq := &apiv1.Query{
		Tokens:        q.Tokens,
		Limit:         int64(q.Limit),
		ContextBefore: int32(q.ContextBefore),
		ContextAfter:  int32(q.ContextAfter),
	}

	if !q.Start.IsZero() {
		pq.Start = timestamppb.New(q.Start)
	}
	if !q.End.IsZero() {
		pq.End = timestamppb.New(q.End)
	}

	if len(q.KV) > 0 {
		pq.KvPredicates = make([]*apiv1.KVPredicate, len(q.KV))
		for i, f := range q.KV {
			pq.KvPredicates[i] = &apiv1.KVPredicate{
				Key:   f.Key,
				Value: f.Value,
			}
		}
	}

	return pq
}

func resumeTokenToBytes(token *query.ResumeToken) []byte {
	if token == nil {
		return nil
	}
	// Simple encoding: chunkID (16 bytes) + recordPos (8 bytes)
	buf := make([]byte, 24)
	copy(buf[:16], token.Next.ChunkID[:])
	binary.BigEndian.PutUint64(buf[16:], token.Next.Pos)
	return buf
}

func bytesToResumeToken(data []byte) *query.ResumeToken {
	if len(data) < 24 {
		return nil
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	pos := binary.BigEndian.Uint64(data[16:])
	return &query.ResumeToken{
		Next: chunk.RecordRef{
			ChunkID: id,
			Pos:     pos,
		},
	}
}

// Conversion helpers: proto -> internal

func protoToRecord(rec *apiv1.Record) chunk.Record {
	if rec == nil {
		return chunk.Record{}
	}
	r := chunk.Record{
		Attrs: rec.Attrs,
		Raw:   rec.Raw,
	}
	if rec.IngestTs != nil {
		r.IngestTS = rec.IngestTs.AsTime()
	}
	if rec.WriteTs != nil {
		r.WriteTS = rec.WriteTs.AsTime()
	}
	return r
}

func protoToChunkMeta(meta *apiv1.ChunkMeta) chunk.ChunkMeta {
	if meta == nil {
		return chunk.ChunkMeta{}
	}
	id, _ := chunk.ParseChunkID(meta.Id)
	m := chunk.ChunkMeta{
		ID:          id,
		Sealed:      meta.Sealed,
		RecordCount: meta.RecordCount,
	}
	if meta.StartTs != nil {
		m.StartTS = meta.StartTs.AsTime()
	}
	if meta.EndTs != nil {
		m.EndTS = meta.EndTs.AsTime()
	}
	return m
}

func protoStepsToInternal(steps []*apiv1.PipelineStep) []query.PipelineStep {
	out := make([]query.PipelineStep, len(steps))
	for i, s := range steps {
		out[i] = query.PipelineStep{
			Index:           s.Name,
			Predicate:       s.Predicate,
			PositionsBefore: int(s.InputEstimate),
			PositionsAfter:  int(s.OutputEstimate),
			Action:          s.Action,
			Reason:          s.Reason,
			Details:         s.Detail,
		}
	}
	return out
}

func protoToQueryPlan(resp *apiv1.ExplainResponse) *query.QueryPlan {
	if resp == nil {
		return nil
	}

	chunks := make([]query.ChunkPlan, len(resp.Chunks))
	for i, cp := range resp.Chunks {
		chunkID, _ := chunk.ParseChunkID(cp.ChunkId)

		runtimeFilter := ""
		if len(cp.RuntimeFilters) > 0 {
			runtimeFilter = cp.RuntimeFilters[0]
		}

		var startTS, endTS time.Time
		if cp.StartTs != nil {
			startTS = cp.StartTs.AsTime()
		}
		if cp.EndTs != nil {
			endTS = cp.EndTs.AsTime()
		}

		var branchPlans []query.BranchPlan
		for _, bp := range cp.BranchPlans {
			branchPlans = append(branchPlans, query.BranchPlan{
				BranchExpr:    bp.Expression,
				Pipeline:      protoStepsToInternal(bp.Steps),
				Skipped:       bp.Skipped,
				SkipReason:    bp.SkipReason,
				EstimatedScan: int(bp.EstimatedRecords),
			})
		}

		storeID, _ := uuid.Parse(cp.StoreId)
		chunks[i] = query.ChunkPlan{
			StoreID:       storeID,
			ChunkID:       chunkID,
			Sealed:        cp.Sealed,
			RecordCount:   int(cp.RecordCount),
			StartTS:       startTS,
			EndTS:         endTS,
			Pipeline:      protoStepsToInternal(cp.Steps),
			BranchPlans:   branchPlans,
			ScanMode:      cp.ScanMode,
			SkipReason:    cp.SkipReason,
			EstimatedScan: int(cp.EstimatedRecords),
			RuntimeFilter: runtimeFilter,
		}
	}

	return &query.QueryPlan{
		Direction:   resp.Direction,
		TotalChunks: int(resp.TotalChunks),
		ChunkPlans:  chunks,
	}
}

func protoToChunkAnalysis(ca *apiv1.ChunkAnalysis) *analyzer.ChunkAnalysis {
	if ca == nil {
		return nil
	}

	chunkID, _ := chunk.ParseChunkID(ca.ChunkId)

	summaries := make([]analyzer.IndexSummary, len(ca.Indexes))
	for i, idx := range ca.Indexes {
		var status analyzer.IndexStatus
		switch idx.Status {
		case "ok", "enabled":
			status = analyzer.StatusEnabled
		case "missing", "disabled":
			status = analyzer.StatusDisabled
		case "incomplete", "capped", "partial":
			status = analyzer.StatusPartial
		default:
			status = analyzer.StatusError
		}

		summaries[i] = analyzer.IndexSummary{
			IndexType:      analyzer.IndexType(idx.Name),
			Status:         status,
			PercentOfChunk: idx.Coverage,
		}
	}

	return &analyzer.ChunkAnalysis{
		ChunkID:      chunkID,
		ChunkRecords: ca.RecordCount,
		Sealed:       ca.Sealed,
		Summaries:    summaries,
	}
}

func protoToAggregateAnalysis(analyses []*apiv1.ChunkAnalysis) *analyzer.AggregateAnalysis {
	if analyses == nil {
		return nil
	}

	chunks := make([]analyzer.ChunkAnalysis, len(analyses))
	for i, ca := range analyses {
		if converted := protoToChunkAnalysis(ca); converted != nil {
			chunks[i] = *converted
		}
	}

	return &analyzer.AggregateAnalysis{
		ChunksAnalyzed: int64(len(analyses)),
		Chunks:         chunks,
	}
}
