package cluster

import (
	"context"
	"errors"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// Forwarder sends pre-marshaled ConfigCommand bytes to the current Raft leader's
// cluster port via the ForwardApply RPC. Used by raftstore.Store on follower
// nodes to transparently proxy config writes.
type Forwarder struct {
	raft  *hraft.Raft
	peers *PeerConns
}

// NewForwarder creates a Forwarder that resolves the leader from r and dials
// through the shared PeerConns pool.
func NewForwarder(r *hraft.Raft, peers *PeerConns) *Forwarder {
	return &Forwarder{raft: r, peers: peers}
}

// Forward sends a pre-marshaled ConfigCommand to the leader for raft.Apply().
// Returns the Raft log index at which the leader applied the command, so the
// follower can wait for its own FSM to catch up before reading post-mutation
// state (gastrolog-2nxij).
//
// Always bounded by ReplicationTimeout even if the caller's ctx has no
// deadline: auth/login HTTP handlers pass a no-deadline ctx, and without
// this bound the RPC hangs indefinitely when the leader (or its
// connection) is frozen. See gastrolog-5oofa.
func (f *Forwarder) Forward(ctx context.Context, data []byte) (uint64, error) {
	addr, leaderID := f.raft.LeaderWithID()
	if addr == "" {
		return 0, errors.New("no known leader")
	}

	conn, err := f.peers.ConnForAddress(addr)
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, ReplicationTimeout)
	defer cancel()
	client := NewForwardApplyClient(conn)
	resp, err := client.ForwardApply(ctx, &gastrologv1.ForwardApplyRequest{Command: data})
	if err != nil {
		if leaderID != "" {
			f.peers.Invalidate(string(leaderID), err)
		}
		return 0, err
	}
	return resp.GetAppliedIndex(), nil
}

// Close is a no-op — connection lifecycle is managed by PeerConns.
func (f *Forwarder) Close() error { return nil }

// ConnForLeader returns a shared connection to the current config Raft leader.
func (p *PeerConns) ConnForLeader(r *hraft.Raft) (*grpc.ClientConn, error) {
	addr, _ := r.LeaderWithID()
	if addr == "" {
		return nil, errors.New("no known leader")
	}
	return p.ConnForAddress(addr)
}
