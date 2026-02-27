package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Forwarder sends pre-marshaled ConfigCommand bytes to the current Raft leader's
// cluster port via the ForwardApply RPC. Used by raftstore.Store on follower
// nodes to transparently proxy config writes.
type Forwarder struct {
	raft *hraft.Raft

	mu   sync.Mutex
	conn *grpc.ClientConn
	last hraft.ServerAddress // cached leader address
}

// NewForwarder creates a Forwarder that resolves the leader from r.
func NewForwarder(r *hraft.Raft) *Forwarder {
	return &Forwarder{raft: r}
}

// Forward sends a pre-marshaled ConfigCommand to the leader for raft.Apply().
func (f *Forwarder) Forward(ctx context.Context, data []byte) error {
	conn, err := f.leaderConn()
	if err != nil {
		return err
	}
	client := NewForwardApplyClient(conn)
	_, err = client.ForwardApply(ctx, &gastrologv1.ForwardApplyRequest{Command: data})
	return err
}

func (f *Forwarder) leaderConn() (*grpc.ClientConn, error) {
	addr, _ := f.raft.LeaderWithID()
	if addr == "" {
		return nil, errors.New("no known leader")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Reuse existing connection if leader hasn't changed.
	if f.conn != nil && f.last == addr {
		return f.conn, nil
	}
	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial leader %s: %w", addr, err)
	}
	f.conn = conn
	f.last = addr
	return conn, nil
}

// Close closes the cached connection to the leader.
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		err := f.conn.Close()
		f.conn = nil
		return err
	}
	return nil
}
