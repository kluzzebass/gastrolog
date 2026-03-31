package multiraft

import (
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcAPI handles incoming Raft RPCs and dispatches them to the correct
// group's rpcChan based on the group_id field in each message.
type grpcAPI[K comparable] struct {
	transport *Transport[K]
}

// handleRPC dispatches a decoded Raft command to the correct group.
// groupID arrives as []byte from the proto wire format; decoded to K for lookup.
func (g *grpcAPI[K]) handleRPC(groupID []byte, command any, data io.Reader) (any, error) {
	gs := g.transport.getGroup(g.transport.decodeKey(groupID))
	if gs == nil {
		return nil, status.Errorf(codes.NotFound, "raft group %x not registered", groupID)
	}

	ch := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  command,
		RespChan: ch,
		Reader:   data,
	}

	dispatched := false
	if req, ok := command.(*raft.AppendEntriesRequest); ok && isHeartbeat(req) {
		gs.heartbeatFuncMtx.Lock()
		fn := gs.heartbeatFunc
		gs.heartbeatFuncMtx.Unlock()
		if fn != nil {
			fn(rpc)
			dispatched = true
		}
	}

	if !dispatched {
		select {
		case gs.rpcChan <- rpc:
		case <-gs.doneCh:
			return nil, status.Error(codes.Unavailable, "raft group removed")
		case <-g.transport.shutdownCh:
			return nil, raft.ErrTransportShutdown
		}
	}

	select {
	case resp := <-ch:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Response, nil
	case <-gs.doneCh:
		return nil, status.Error(codes.Unavailable, "raft group removed")
	case <-g.transport.shutdownCh:
		return nil, raft.ErrTransportShutdown
	}
}

// ---------- Unary handlers ----------

func (g *grpcAPI[K]) appendEntries(req *gastrologv1.MultiRaftAppendEntriesRequest) (*gastrologv1.MultiRaftAppendEntriesResponse, error) {
	resp, err := g.handleRPC(req.GetGroupId(), decodeAppendEntriesRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse)), nil
}

func (g *grpcAPI[K]) requestVote(req *gastrologv1.MultiRaftRequestVoteRequest) (*gastrologv1.MultiRaftRequestVoteResponse, error) {
	resp, err := g.handleRPC(req.GetGroupId(), decodeRequestVoteRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeRequestVoteResponse(resp.(*raft.RequestVoteResponse)), nil
}

func (g *grpcAPI[K]) requestPreVote(req *gastrologv1.MultiRaftRequestPreVoteRequest) (*gastrologv1.MultiRaftRequestPreVoteResponse, error) {
	resp, err := g.handleRPC(req.GetGroupId(), decodeRequestPreVoteRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeRequestPreVoteResponse(resp.(*raft.RequestPreVoteResponse)), nil
}

func (g *grpcAPI[K]) timeoutNow(req *gastrologv1.MultiRaftTimeoutNowRequest) (*gastrologv1.MultiRaftTimeoutNowResponse, error) {
	resp, err := g.handleRPC(req.GetGroupId(), decodeTimeoutNowRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeTimeoutNowResponse(resp.(*raft.TimeoutNowResponse)), nil
}

// ---------- Snapshot stream ----------

// snapshotStream adapts gRPC server-side streaming into an io.Reader for
// InstallSnapshot. The first message carries metadata; subsequent messages
// carry snapshot data chunks.
type snapshotStream struct {
	recv func() (*gastrologv1.MultiRaftInstallSnapshotRequest, error)
	buf  []byte
}

func (s *snapshotStream) Read(b []byte) (int, error) {
	if len(s.buf) > 0 {
		n := copy(b, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	m, err := s.recv()
	if err != nil {
		return 0, err
	}
	n := copy(b, m.GetData())
	if n < len(m.GetData()) {
		s.buf = m.GetData()[n:]
	}
	return n, nil
}
