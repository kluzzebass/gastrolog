package cluster

import (
	"context"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EnrollHandler is a callback for the Enroll RPC. The handler is responsible
// for verifying the token, calling AddVoter, and returning TLS material.
// Registered via Server.SetEnrollHandler().
type EnrollHandler func(ctx context.Context, req *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error)

// SetEnrollHandler registers the callback invoked when a joining node calls Enroll.
func (s *Server) SetEnrollHandler(h EnrollHandler) {
	s.enrollHandler = h
}

// enroll delegates to the registered EnrollHandler.
func (s *Server) enroll(ctx context.Context, req *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error) {
	if s.enrollHandler == nil {
		return nil, status.Error(codes.Unavailable, "enroll handler not configured")
	}
	return s.enrollHandler(ctx, req)
}

// enrollRPCHandler is the gRPC MethodDesc handler for the Enroll RPC.
func enrollRPCHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.EnrollRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.enroll(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/Enroll",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.enroll(ctx, req.(*gastrologv1.EnrollRequest))
	}
	return interceptor(ctx, req, info, handler)
}
