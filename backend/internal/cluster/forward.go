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
)

// RecordAppender appends a single record to a local vault.
// Used by the ForwardRecords handler to write received records.
type RecordAppender func(ctx context.Context, vaultID uuid.UUID, rec chunk.Record) error

// SetRecordAppender injects the callback for writing forwarded records.
// Must be called before the cluster server receives ForwardRecords RPCs.
func (s *Server) SetRecordAppender(fn RecordAppender) {
	s.recordAppender = fn
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
	},
}

// clusterServiceServer is the interface the gRPC runtime uses for type-checking.
type clusterServiceServer interface {
	forwardApply(context.Context, *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error)
	enroll(context.Context, *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error)
	broadcast(context.Context, *gastrologv1.BroadcastRequest) (*gastrologv1.BroadcastResponse, error)
	forwardRecords(context.Context, *gastrologv1.ForwardRecordsRequest) (*gastrologv1.ForwardRecordsResponse, error)
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
