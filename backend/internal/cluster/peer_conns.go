package cluster

import (
	"fmt"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerConns manages a shared pool of gRPC connections to cluster peers.
// All cluster components (Broadcaster, RecordForwarder, SearchForwarder)
// share a single PeerConns so that traffic to each peer is multiplexed
// over one connection.
type PeerConns struct {
	raft       *hraft.Raft
	clusterTLS *ClusterTLS
	nodeID     string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPeerConns creates a shared peer connection pool.
func NewPeerConns(r *hraft.Raft, clusterTLS *ClusterTLS, nodeID string) *PeerConns {
	return &PeerConns{
		raft:       r,
		clusterTLS: clusterTLS,
		nodeID:     nodeID,
		conns:      make(map[string]*grpc.ClientConn),
	}
}

// Conn returns a cached or newly dialed gRPC connection for the given node.
func (p *PeerConns) Conn(nodeID string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[nodeID]; ok {
		return conn, nil
	}

	addr, err := p.resolveAddr(nodeID)
	if err != nil {
		return nil, err
	}

	var creds credentials.TransportCredentials
	if p.clusterTLS != nil && p.clusterTLS.State() != nil {
		creds = p.clusterTLS.TransportCredentials()
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  500 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial node %s at %s: %w", nodeID, addr, err)
	}
	p.conns[nodeID] = conn
	return conn, nil
}

// Invalidate closes and removes the cached connection for a node,
// forcing a fresh dial on the next Conn call.
func (p *PeerConns) Invalidate(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[nodeID]; ok {
		_ = conn.Close()
		delete(p.conns, nodeID)
	}
}

// Peers returns all Raft servers except self.
func (p *PeerConns) Peers() ([]hraft.Server, error) {
	future := p.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var peers []hraft.Server
	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) != p.nodeID {
			peers = append(peers, srv)
		}
	}
	return peers, nil
}

// Reset swaps the raft pointer and closes all cached connections,
// forcing fresh dials on the next Conn call. Components holding a *PeerConns
// reference (Broadcaster, RecordForwarder, SearchForwarder) automatically
// use the new Raft instance without recreation.
func (p *PeerConns) Reset(r *hraft.Raft) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raft = r
	for id, conn := range p.conns {
		_ = conn.Close()
		delete(p.conns, id)
	}
}

// Close tears down all cached connections.
func (p *PeerConns) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, conn := range p.conns {
		_ = conn.Close()
		delete(p.conns, id)
	}
	return nil
}

// resolveAddr looks up the node's address from the Raft configuration.
func (p *PeerConns) resolveAddr(nodeID string) (string, error) {
	future := p.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return "", fmt.Errorf("get raft config: %w", err)
	}
	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) == nodeID {
			return string(srv.Address), nil
		}
	}
	return "", fmt.Errorf("node %s not found in raft config", nodeID)
}
