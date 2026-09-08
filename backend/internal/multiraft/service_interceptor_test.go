package multiraft

import (
	"context"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// stubDispatcher answers every raft RPC with an empty response. The subject
// here is the MethodDesc plumbing, not consensus.
type stubDispatcher struct{}

func (stubDispatcher) appendEntries(*gastrologv1.MultiRaftAppendEntriesRequest) (*gastrologv1.MultiRaftAppendEntriesResponse, error) {
	return &gastrologv1.MultiRaftAppendEntriesResponse{}, nil
}

func (stubDispatcher) batchHeartbeat(*gastrologv1.MultiRaftBatchHeartbeatRequest) (*gastrologv1.MultiRaftBatchHeartbeatResponse, error) {
	return &gastrologv1.MultiRaftBatchHeartbeatResponse{}, nil
}

func (stubDispatcher) requestVote(*gastrologv1.MultiRaftRequestVoteRequest) (*gastrologv1.MultiRaftRequestVoteResponse, error) {
	return &gastrologv1.MultiRaftRequestVoteResponse{}, nil
}

func (stubDispatcher) requestPreVote(*gastrologv1.MultiRaftRequestPreVoteRequest) (*gastrologv1.MultiRaftRequestPreVoteResponse, error) {
	return &gastrologv1.MultiRaftRequestPreVoteResponse{}, nil
}

func (stubDispatcher) timeoutNow(*gastrologv1.MultiRaftTimeoutNowRequest) (*gastrologv1.MultiRaftTimeoutNowResponse, error) {
	return &gastrologv1.MultiRaftTimeoutNowResponse{}, nil
}

func (stubDispatcher) handleRPC(_ []byte, command any, _ io.Reader) (any, error) {
	switch command.(type) {
	case *raft.InstallSnapshotRequest:
		return &raft.InstallSnapshotResponse{}, nil
	default:
		return &raft.AppendEntriesResponse{}, nil
	}
}

// methodRecorder collects the FullMethod every interceptor invocation observes.
type methodRecorder struct {
	mu     sync.Mutex
	unary  map[string]bool
	stream map[string]bool
}

func (r *methodRecorder) record(set map[string]bool, method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	set[method] = true
}

func (r *methodRecorder) snapshot(set map[string]bool) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Sorted(maps.Keys(set))
}

// TestServiceDescRunsServerInterceptors pins that every method on
// MultiRaftTransportService reaches the server's interceptor chain. The
// handlers are hand-written rather than generated: gRPC passes a unary handler
// its interceptor chain as an argument and applies nothing itself, so a handler
// that ignores that argument serves raft RPCs with authentication switched off
// while every dial-level test still passes. Both expectation sets are derived
// from serviceDesc, so a method added without that plumbing fails here.
func TestServiceDescRunsServerInterceptors(t *testing.T) {
	t.Parallel()

	rec := &methodRecorder{unary: map[string]bool{}, stream: map[string]bool{}}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			rec.record(rec.unary, info.FullMethod)
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			rec.record(rec.stream, info.FullMethod)
			return handler(srv, ss)
		}),
	)
	srv.RegisterService(&serviceDesc, stubDispatcher{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wantUnary, wantStream []string

	// An empty message decodes into any request type, so the calls can be
	// driven straight off the descriptor instead of a hand-kept list.
	for _, m := range serviceDesc.Methods {
		full := "/" + serviceDesc.ServiceName + "/" + m.MethodName
		wantUnary = append(wantUnary, full)
		if err := conn.Invoke(ctx, full, &emptypb.Empty{}, &emptypb.Empty{}); err != nil {
			t.Fatalf("invoke %s: %v", full, err)
		}
	}

	for _, s := range serviceDesc.Streams {
		full := "/" + serviceDesc.ServiceName + "/" + s.StreamName
		wantStream = append(wantStream, full)
		desc := &grpc.StreamDesc{
			StreamName:    s.StreamName,
			ServerStreams: s.ServerStreams,
			ClientStreams: s.ClientStreams,
		}
		st, err := conn.NewStream(ctx, desc, full)
		if err != nil {
			t.Fatalf("open stream %s: %v", full, err)
		}
		if err := st.SendMsg(&emptypb.Empty{}); err != nil {
			t.Fatalf("send on %s: %v", full, err)
		}
		if err := st.CloseSend(); err != nil {
			t.Fatalf("close send on %s: %v", full, err)
		}
		// Returning from RecvMsg means the server produced the reply, which it
		// can only do after the interceptor let the handler run.
		if err := st.RecvMsg(&emptypb.Empty{}); err != nil {
			t.Fatalf("recv on %s: %v", full, err)
		}
	}

	slices.Sort(wantUnary)
	slices.Sort(wantStream)

	if got := rec.snapshot(rec.unary); !slices.Equal(got, wantUnary) {
		t.Errorf("unary interceptor observed %v, want %v — a method bypasses the interceptor chain", got, wantUnary)
	}
	if got := rec.snapshot(rec.stream); !slices.Equal(got, wantStream) {
		t.Errorf("stream interceptor observed %v, want %v — a stream bypasses the interceptor chain", got, wantStream)
	}
}
