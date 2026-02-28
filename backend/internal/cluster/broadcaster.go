package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Broadcaster fans out BroadcastMessages to all cluster peers via the
// Broadcast RPC. Unlike Forwarder (leader-only), Broadcaster maintains
// connections to every peer in the Raft configuration.
type Broadcaster struct {
	raft       *hraft.Raft
	clusterTLS *ClusterTLS
	nodeID     string
	logger     *slog.Logger

	mu    sync.Mutex
	conns map[hraft.ServerID]*grpc.ClientConn
}

// NewBroadcaster creates a Broadcaster that resolves peers from the Raft
// configuration. If clusterTLS is non-nil, connections use mTLS.
func NewBroadcaster(r *hraft.Raft, clusterTLS *ClusterTLS, nodeID string, logger *slog.Logger) *Broadcaster {
	return &Broadcaster{
		raft:       r,
		clusterTLS: clusterTLS,
		nodeID:     nodeID,
		logger:     logger,
		conns:      make(map[hraft.ServerID]*grpc.ClientConn),
	}
}

// Send broadcasts a message to all peers. Best-effort: errors are logged
// but don't block delivery to other peers.
func (b *Broadcaster) Send(ctx context.Context, msg *gastrologv1.BroadcastMessage) {
	peers, err := b.peers()
	if err != nil {
		b.logger.Warn("broadcast: get peers", "error", err)
		return
	}
	if len(peers) == 0 {
		return
	}

	req := &gastrologv1.BroadcastRequest{Message: msg}
	for _, p := range peers {
		conn, err := b.peerConn(p)
		if err != nil {
			b.logger.Warn("broadcast: dial peer", "peer", p.ID, "error", err)
			continue
		}
		if err := b.sendOne(ctx, conn, p, req); err != nil {
			b.logger.Warn("broadcast: send", "peer", p.ID, "error", err)
			b.invalidate(p.ID)
		}
	}
}

// Close tears down all cached connections.
func (b *Broadcaster) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, conn := range b.conns {
		_ = conn.Close()
		delete(b.conns, id)
	}
	return nil
}

// peers returns all Raft servers except self.
func (b *Broadcaster) peers() ([]hraft.Server, error) {
	future := b.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var peers []hraft.Server
	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) != b.nodeID {
			peers = append(peers, srv)
		}
	}
	return peers, nil
}

// peerConn returns a cached or newly dialed connection for the given peer.
func (b *Broadcaster) peerConn(p hraft.Server) (*grpc.ClientConn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if conn, ok := b.conns[p.ID]; ok {
		return conn, nil
	}

	var creds credentials.TransportCredentials
	if b.clusterTLS != nil && b.clusterTLS.State() != nil {
		creds = b.clusterTLS.TransportCredentials()
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(string(p.Address),
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s at %s: %w", p.ID, p.Address, err)
	}
	b.conns[p.ID] = conn
	return conn, nil
}

// invalidate closes and removes the cached connection for a peer,
// forcing a redial on the next Send.
func (b *Broadcaster) invalidate(id hraft.ServerID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if conn, ok := b.conns[id]; ok {
		_ = conn.Close()
		delete(b.conns, id)
	}
}

// sendOne sends a BroadcastRequest to a single peer via the manually-defined
// RPC path (same pattern as ForwardApplyClient).
func (b *Broadcaster) sendOne(ctx context.Context, conn *grpc.ClientConn, p hraft.Server, req *gastrologv1.BroadcastRequest) error {
	out := &gastrologv1.BroadcastResponse{}
	return conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Broadcast", req, out)
}
