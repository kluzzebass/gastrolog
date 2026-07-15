package multiraft

import (
	"bytes"
	"io"
	"sync"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// groupLaneAPI serves MultiRaftTransportService on a dedicated per-group gRPC
// stack. RPCs whose group_id does not match the lane group are rejected.
type groupLaneAPI[K comparable] struct {
	transport *Transport[K]
	groupID   K
	wireGroup []byte
}

func newGroupLaneAPI[K comparable](t *Transport[K], groupID K) *groupLaneAPI[K] {
	return &groupLaneAPI[K]{
		transport: t,
		groupID:   groupID,
		wireGroup: t.encodeKey(groupID),
	}
}

func (g *groupLaneAPI[K]) checkGroupID(wire []byte) error {
	if len(wire) == 0 || !bytes.Equal(wire, g.wireGroup) {
		return status.Errorf(codes.InvalidArgument, "raft RPC group_id %q does not match lane group %q",
			string(wire), g.transport.groupIDString(g.groupID))
	}
	return nil
}

func (g *groupLaneAPI[K]) appendEntries(req *gastrologv1.MultiRaftAppendEntriesRequest) (*gastrologv1.MultiRaftAppendEntriesResponse, error) {
	if err := g.checkGroupID(req.GetGroupId()); err != nil {
		return nil, err
	}
	resp, err := g.transport.dispatchRPC(g.groupID, decodeAppendEntriesRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse)), nil
}

func (g *groupLaneAPI[K]) requestVote(req *gastrologv1.MultiRaftRequestVoteRequest) (*gastrologv1.MultiRaftRequestVoteResponse, error) {
	if err := g.checkGroupID(req.GetGroupId()); err != nil {
		return nil, err
	}
	resp, err := g.transport.dispatchRPC(g.groupID, decodeRequestVoteRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeRequestVoteResponse(resp.(*raft.RequestVoteResponse)), nil
}

func (g *groupLaneAPI[K]) requestPreVote(req *gastrologv1.MultiRaftRequestPreVoteRequest) (*gastrologv1.MultiRaftRequestPreVoteResponse, error) {
	if err := g.checkGroupID(req.GetGroupId()); err != nil {
		return nil, err
	}
	resp, err := g.transport.dispatchRPC(g.groupID, decodeRequestPreVoteRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeRequestPreVoteResponse(resp.(*raft.RequestPreVoteResponse)), nil
}

func (g *groupLaneAPI[K]) timeoutNow(req *gastrologv1.MultiRaftTimeoutNowRequest) (*gastrologv1.MultiRaftTimeoutNowResponse, error) {
	if err := g.checkGroupID(req.GetGroupId()); err != nil {
		return nil, err
	}
	resp, err := g.transport.dispatchRPC(g.groupID, decodeTimeoutNowRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeTimeoutNowResponse(resp.(*raft.TimeoutNowResponse)), nil
}

func (g *groupLaneAPI[K]) batchHeartbeat(req *gastrologv1.MultiRaftBatchHeartbeatRequest) (*gastrologv1.MultiRaftBatchHeartbeatResponse, error) {
	hbs := req.GetHeartbeats()
	responses := make([]*gastrologv1.MultiRaftAppendEntriesResponse, len(hbs))
	var wg sync.WaitGroup
	for i, hb := range hbs {
		wg.Add(1)
		go func(i int, hb *gastrologv1.MultiRaftAppendEntriesRequest) {
			defer wg.Done()
			resp, err := g.appendEntries(hb)
			if err != nil {
				responses[i] = &gastrologv1.MultiRaftAppendEntriesResponse{}
				return
			}
			responses[i] = resp
		}(i, hb)
	}
	wg.Wait()
	return &gastrologv1.MultiRaftBatchHeartbeatResponse{Responses: responses}, nil
}

func (g *groupLaneAPI[K]) handleRPC(groupID []byte, command any, data io.Reader) (any, error) {
	if err := g.checkGroupID(groupID); err != nil {
		return nil, err
	}
	return g.transport.dispatchRPC(g.groupID, command, data)
}
