package multiraft

import (
	"context"
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// raftDispatcher is the interface used by gRPC handler functions to dispatch
// RPCs without knowing the concrete generic type parameter K.
type raftDispatcher interface {
	appendEntries(req *gastrologv1.MultiRaftAppendEntriesRequest) (*gastrologv1.MultiRaftAppendEntriesResponse, error)
	batchHeartbeat(req *gastrologv1.MultiRaftBatchHeartbeatRequest) (*gastrologv1.MultiRaftBatchHeartbeatResponse, error)
	requestVote(req *gastrologv1.MultiRaftRequestVoteRequest) (*gastrologv1.MultiRaftRequestVoteResponse, error)
	requestPreVote(req *gastrologv1.MultiRaftRequestPreVoteRequest) (*gastrologv1.MultiRaftRequestPreVoteResponse, error)
	timeoutNow(req *gastrologv1.MultiRaftTimeoutNowRequest) (*gastrologv1.MultiRaftTimeoutNowResponse, error)
	handleRPC(groupID []byte, command any, data io.Reader) (any, error)
}

// Register registers the MultiRaftTransportService on a gRPC server.
// Used for single-stack (non-TLS / test) mode where all groups share one server.
func (t *Transport[K]) Register(s grpc.ServiceRegistrar) {
	s.RegisterService(&serviceDesc, &grpcAPI[K]{transport: t})
}

// RegisterGroup registers MultiRaftTransportService on a dedicated per-group
// gRPC stack (isolated inbound raft lane).
func (t *Transport[K]) RegisterGroup(s grpc.ServiceRegistrar, groupID K) {
	s.RegisterService(&serviceDesc, newGroupLaneAPI(t, groupID))
}

const serviceName = "gastrolog.v1.MultiRaftTransportService"

var serviceDesc = grpc.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*raftDispatcher)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "AppendEntries", Handler: handleAppendEntries},
		{MethodName: "RequestVote", Handler: handleRequestVote},
		{MethodName: "RequestPreVote", Handler: handleRequestPreVote},
		{MethodName: "TimeoutNow", Handler: handleTimeoutNow},
		{MethodName: "BatchHeartbeat", Handler: handleBatchHeartbeat},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "InstallSnapshot",
			Handler:       handleInstallSnapshot,
			ClientStreams: true,
		},
		{
			StreamName:    "AppendEntriesPipeline",
			Handler:       handleAppendEntriesPipeline,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
}

// throughInterceptor dispatches req under the server's unary interceptor chain.
// gRPC hands a hand-written MethodDesc handler the chain as an argument and
// applies nothing itself, so a handler that drops it silently disables every
// server-level unary interceptor on this service — mTLS enforcement included.
func throughInterceptor[Req, Resp any](
	ctx context.Context,
	srv any,
	req *Req,
	method string,
	interceptor grpc.UnaryServerInterceptor,
	dispatch func(*Req) (*Resp, error),
) (any, error) {
	handler := func(_ context.Context, r any) (any, error) {
		resp, err := dispatch(r.(*Req))
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	if interceptor == nil {
		return handler(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + serviceName + "/" + method}
	return interceptor(ctx, req, info, handler)
}

func handleAppendEntries(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftAppendEntriesRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return throughInterceptor(ctx, srv, req, "AppendEntries", interceptor, srv.(raftDispatcher).appendEntries)
}

func handleRequestVote(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftRequestVoteRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return throughInterceptor(ctx, srv, req, "RequestVote", interceptor, srv.(raftDispatcher).requestVote)
}

func handleRequestPreVote(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftRequestPreVoteRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return throughInterceptor(ctx, srv, req, "RequestPreVote", interceptor, srv.(raftDispatcher).requestPreVote)
}

func handleTimeoutNow(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftTimeoutNowRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return throughInterceptor(ctx, srv, req, "TimeoutNow", interceptor, srv.(raftDispatcher).timeoutNow)
}

func handleBatchHeartbeat(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftBatchHeartbeatRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return throughInterceptor(ctx, srv, req, "BatchHeartbeat", interceptor, srv.(raftDispatcher).batchHeartbeat)
}

func handleInstallSnapshot(srv any, stream grpc.ServerStream) error {
	api := srv.(raftDispatcher)
	first := new(gastrologv1.MultiRaftInstallSnapshotRequest)
	if err := stream.RecvMsg(first); err != nil {
		return err
	}
	reader := &snapshotStream{
		recv: func() (*gastrologv1.MultiRaftInstallSnapshotRequest, error) {
			msg := new(gastrologv1.MultiRaftInstallSnapshotRequest)
			if err := stream.RecvMsg(msg); err != nil {
				return nil, err
			}
			return msg, nil
		},
		buf: first.GetData(),
	}
	resp, err := api.handleRPC(first.GetGroupId(), decodeInstallSnapshotRequest(first), reader)
	if err != nil {
		return err
	}
	return stream.SendMsg(encodeInstallSnapshotResponse(resp.(*raft.InstallSnapshotResponse)))
}

func handleAppendEntriesPipeline(srv any, stream grpc.ServerStream) error {
	api := srv.(raftDispatcher)
	for {
		msg := new(gastrologv1.MultiRaftAppendEntriesRequest)
		if err := stream.RecvMsg(msg); err != nil {
			return err
		}
		resp, err := api.handleRPC(msg.GetGroupId(), decodeAppendEntriesRequest(msg), nil)
		if err != nil {
			return err
		}
		if err := stream.SendMsg(encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse))); err != nil {
			return err
		}
	}
}
