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
func (t *Transport[K]) Register(s grpc.ServiceRegistrar) {
	s.RegisterService(&serviceDesc, &grpcAPI[K]{transport: t})
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "gastrolog.v1.MultiRaftTransportService",
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

func handleAppendEntries(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftAppendEntriesRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(raftDispatcher).appendEntries(req)
}

func handleRequestVote(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftRequestVoteRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(raftDispatcher).requestVote(req)
}

func handleRequestPreVote(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftRequestPreVoteRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(raftDispatcher).requestPreVote(req)
}

func handleTimeoutNow(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftTimeoutNowRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(raftDispatcher).timeoutNow(req)
}

func handleBatchHeartbeat(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	req := new(gastrologv1.MultiRaftBatchHeartbeatRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(raftDispatcher).batchHeartbeat(req)
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
