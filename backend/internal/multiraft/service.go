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
		unaryMethod("AppendEntries", raftDispatcher.appendEntries),
		unaryMethod("RequestVote", raftDispatcher.requestVote),
		unaryMethod("RequestPreVote", raftDispatcher.requestPreVote),
		unaryMethod("TimeoutNow", raftDispatcher.timeoutNow),
		unaryMethod("BatchHeartbeat", raftDispatcher.batchHeartbeat),
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

// unaryMethod builds the MethodDesc for one unary raft RPC. name is written
// once and serves as both the registered method name and the FullMethod the
// interceptor chain sees, so the two cannot drift — requireClientCert matches
// its exemption against FullMethod.
//
// The handler runs dispatch through the interceptor chain gRPC passes in.
// gRPC applies nothing itself for hand-written MethodDescs, so a handler that
// drops that argument silently disables every server-level unary interceptor
// on this service — mTLS enforcement included.
func unaryMethod[Req, Resp any](name string, dispatch func(raftDispatcher, *Req) (*Resp, error)) grpc.MethodDesc {
	return grpc.MethodDesc{
		MethodName: name,
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := new(Req)
			if err := dec(req); err != nil {
				return nil, err
			}
			// Dispatch takes no context: a raft RPC hands off to the group's
			// rpcChan and the reply comes back on a channel, so there is no
			// downstream call for a deadline or span to be threaded into.
			handler := func(_ context.Context, r any) (any, error) {
				resp, err := dispatch(srv.(raftDispatcher), r.(*Req))
				if err != nil {
					return nil, err
				}
				return resp, nil
			}
			if interceptor == nil {
				return handler(ctx, req)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + serviceName + "/" + name}
			return interceptor(ctx, req, info, handler)
		},
	}
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
