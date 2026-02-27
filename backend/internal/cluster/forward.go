package cluster

import (
	"context"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

// forwardServiceDesc is a manually-defined gRPC ServiceDesc for
// gastrolog.v1.ClusterService. We register this manually rather than using
// protoc-gen-go-grpc to avoid generating unused gRPC stubs for all services
// in the proto package.
var forwardServiceDesc = grpc.ServiceDesc{
	ServiceName: "gastrolog.v1.ClusterService",
	HandlerType: (*forwardServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ForwardApply",
			Handler:    forwardApplyHandler,
		},
	},
}

// forwardServiceServer is the interface the gRPC runtime uses for type-checking.
type forwardServiceServer interface {
	forwardApply(context.Context, *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error)
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

func registerForwardService(s *grpc.Server, srv *Server) {
	s.RegisterService(&forwardServiceDesc, srv)
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
