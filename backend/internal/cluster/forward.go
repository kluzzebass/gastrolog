package cluster

import (
	"context"
	"maps"

	"gastrolog/internal/chunk"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RecordAppender appends a single record to a local vault.
// Used by the ForwardRecords handler to write received records.
type RecordAppender func(ctx context.Context, vaultID uuid.UUID, rec chunk.Record) error

// SearchExecutor runs a search on a local vault and returns a batch of records.
// Used by the ForwardSearch handler to serve remote search requests.
type SearchExecutor func(ctx context.Context, vaultID uuid.UUID, queryExpr string, resumeToken []byte) ([]*gastrologv1.ExportRecord, []byte, bool, error)

// ContextExecutor fetches records surrounding a specific position in a local vault.
// Used by the ForwardGetContext handler to serve remote context requests.
type ContextExecutor func(ctx context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID, pos uint64, before, after int) ([]chunk.Record, chunk.Record, []chunk.Record, error)

// SetRecordAppender injects the callback for writing forwarded records.
// Must be called before the cluster server receives ForwardRecords RPCs.
func (s *Server) SetRecordAppender(fn RecordAppender) {
	s.recordAppender = fn
}

// SetSearchExecutor injects the callback for handling remote search requests.
func (s *Server) SetSearchExecutor(fn SearchExecutor) {
	s.searchExecutor = fn
}

// SetContextExecutor injects the callback for handling remote GetContext requests.
func (s *Server) SetContextExecutor(fn ContextExecutor) {
	s.contextExecutor = fn
}

// forwardRecords handles the ForwardRecords RPC. Converts proto ExportRecords
// to chunk.Record and writes them via the RecordAppender callback.
func (s *Server) forwardRecords(ctx context.Context, req *gastrologv1.ForwardRecordsRequest) (*gastrologv1.ForwardRecordsResponse, error) {
	if s.recordAppender == nil {
		return nil, status.Error(codes.Unavailable, "record appender not configured")
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vault_id: %v", err)
	}

	var written int64
	for _, exportRec := range req.GetRecords() {
		rec := chunk.Record{
			Raw: exportRec.GetRaw(),
		}
		if exportRec.GetSourceTs() != nil {
			rec.SourceTS = exportRec.GetSourceTs().AsTime()
		}
		if exportRec.GetIngestTs() != nil {
			rec.IngestTS = exportRec.GetIngestTs().AsTime()
		}
		if len(exportRec.GetAttrs()) > 0 {
			rec.Attrs = make(chunk.Attributes, len(exportRec.GetAttrs()))
			maps.Copy(rec.Attrs, exportRec.GetAttrs())
		}

		if err := s.recordAppender(ctx, vaultID, rec); err != nil {
			return nil, status.Errorf(codes.Internal, "append record: %v", err)
		}
		written++
	}

	return &gastrologv1.ForwardRecordsResponse{RecordsWritten: written}, nil
}

// forwardSearch handles the ForwardSearch RPC. Executes a search on a local
// vault and returns matching records to the requesting node.
func (s *Server) forwardSearch(ctx context.Context, req *gastrologv1.ForwardSearchRequest) (*gastrologv1.ForwardSearchResponse, error) {
	if s.searchExecutor == nil {
		return nil, status.Error(codes.Unavailable, "search executor not configured")
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vault_id: %v", err)
	}
	records, resumeToken, hasMore, err := s.searchExecutor(ctx, vaultID, req.GetQuery(), req.GetResumeToken())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "search: %v", err)
	}
	return &gastrologv1.ForwardSearchResponse{
		Records:     records,
		ResumeToken: resumeToken,
		HasMore:     hasMore,
	}, nil
}

// forwardGetContext handles the ForwardGetContext RPC. Runs GetContext on a
// local vault and returns the anchor + surrounding records to the requesting node.
func (s *Server) forwardGetContext(ctx context.Context, req *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error) {
	if s.contextExecutor == nil {
		return nil, status.Error(codes.Unavailable, "context executor not configured")
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vault_id: %v", err)
	}
	chunkID, err := chunk.ParseChunkID(req.GetChunkId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk_id: %v", err)
	}

	before, anchor, after, err := s.contextExecutor(ctx, vaultID, chunkID, req.GetPos(), int(req.GetBefore()), int(req.GetAfter()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get context: %v", err)
	}

	resp := &gastrologv1.ForwardGetContextResponse{
		Anchor: RecordToExportRecord(anchor),
		Before: make([]*gastrologv1.ExportRecord, len(before)),
		After:  make([]*gastrologv1.ExportRecord, len(after)),
	}
	for i, rec := range before {
		resp.Before[i] = RecordToExportRecord(rec)
	}
	for i, rec := range after {
		resp.After[i] = RecordToExportRecord(rec)
	}
	return resp, nil
}

// RecordToExportRecord converts a chunk.Record to an ExportRecord proto
// with full ref fields. Used by the ForwardGetContext handler and the
// search executor in main.go.
func RecordToExportRecord(rec chunk.Record) *gastrologv1.ExportRecord {
	er := &gastrologv1.ExportRecord{
		Raw:     rec.Raw,
		VaultId: rec.VaultID.String(),
		ChunkId: rec.Ref.ChunkID.String(),
		Pos:     rec.Ref.Pos,
	}
	if !rec.SourceTS.IsZero() {
		er.SourceTs = timestamppb.New(rec.SourceTS)
	}
	if !rec.IngestTS.IsZero() {
		er.IngestTs = timestamppb.New(rec.IngestTS)
	}
	if len(rec.Attrs) > 0 {
		er.Attrs = make(map[string]string, len(rec.Attrs))
		maps.Copy(er.Attrs, rec.Attrs)
	}
	return er
}

// forwardApply handles the ForwardApply RPC on the leader.
// Followers call this to proxy config writes through the leader's raft.Apply().
func (s *Server) forwardApply(ctx context.Context, req *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error) {
	if s.applyFn == nil {
		return nil, status.Error(codes.Unavailable, "apply function not configured")
	}
	if err := s.applyFn(ctx, req.GetCommand()); err != nil {
		return nil, status.Errorf(codes.Internal, "apply: %v", err)
	}
	return &gastrologv1.ForwardApplyResponse{}, nil
}

// clusterServiceDesc is a manually-defined gRPC ServiceDesc for
// gastrolog.v1.ClusterService. We register this manually rather than using
// protoc-gen-go-grpc to avoid generating unused gRPC stubs for all services
// in the proto package.
var clusterServiceDesc = grpc.ServiceDesc{
	ServiceName: "gastrolog.v1.ClusterService",
	HandlerType: (*clusterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ForwardApply",
			Handler:    forwardApplyHandler,
		},
		{
			MethodName: "Enroll",
			Handler:    enrollRPCHandler,
		},
		{
			MethodName: "Broadcast",
			Handler:    broadcastHandler,
		},
		{
			MethodName: "ForwardRecords",
			Handler:    forwardRecordsHandler,
		},
		{
			MethodName: "ForwardSearch",
			Handler:    forwardSearchHandler,
		},
		{
			MethodName: "ForwardGetContext",
			Handler:    forwardGetContextHandler,
		},
	},
}

// clusterServiceServer is the interface the gRPC runtime uses for type-checking.
type clusterServiceServer interface {
	forwardApply(context.Context, *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error)
	enroll(context.Context, *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error)
	broadcast(context.Context, *gastrologv1.BroadcastRequest) (*gastrologv1.BroadcastResponse, error)
	forwardRecords(context.Context, *gastrologv1.ForwardRecordsRequest) (*gastrologv1.ForwardRecordsResponse, error)
	forwardSearch(context.Context, *gastrologv1.ForwardSearchRequest) (*gastrologv1.ForwardSearchResponse, error)
	forwardGetContext(context.Context, *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error)
}

func forwardApplyHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardApplyRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardApply(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardApply",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardApply(ctx, req.(*gastrologv1.ForwardApplyRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardRecordsHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardRecordsRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardRecords(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardRecords",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardRecords(ctx, req.(*gastrologv1.ForwardRecordsRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardSearchHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardSearchRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardSearch(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardSearch",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardSearch(ctx, req.(*gastrologv1.ForwardSearchRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardGetContextHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardGetContextRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardGetContext(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardGetContext",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardGetContext(ctx, req.(*gastrologv1.ForwardGetContextRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func registerClusterService(s *grpc.Server, srv *Server) {
	s.RegisterService(&clusterServiceDesc, srv)
}

// ForwardApplyClient is a client for the ForwardApply RPC.
type ForwardApplyClient struct {
	cc grpc.ClientConnInterface
}

// NewForwardApplyClient creates a client bound to a connection.
func NewForwardApplyClient(cc grpc.ClientConnInterface) *ForwardApplyClient {
	return &ForwardApplyClient{cc: cc}
}

// ForwardApply sends a config command to the leader.
func (c *ForwardApplyClient) ForwardApply(ctx context.Context, req *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error) {
	out := &gastrologv1.ForwardApplyResponse{}
	if err := c.cc.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardApply", req, out); err != nil {
		return nil, err
	}
	return out, nil
}
