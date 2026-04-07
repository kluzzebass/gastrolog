package cluster

import (
	"fmt"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// invalidateGracePeriod is how long Invalidate/Reset wait before closing a
// dropped connection. This grace period gives concurrent goroutines that
// already retrieved the *ClientConn from the cache time to finish their
// in-flight RPCs before the underlying transport is torn down. Without it,
// goroutine A's failure (which calls Invalidate) would synchronously close
// the conn that goroutines B/C/D are still using, causing them to fail with
// "client connection is closing" mid-RPC and lose work.
const invalidateGracePeriod = 5 * time.Second

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
//
// If a cached conn is in connectivity.Shutdown state, it is discarded and
// re-dialed. Shutdown is terminal — gRPC enters it only when something has
// called Close() on the conn — so handing it back guarantees the next RPC
// fails with "client connection is closing". The state check is a safety net
// for any caller that beats the Invalidate grace period.
func (p *PeerConns) Conn(nodeID string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[nodeID]; ok {
		if conn.GetState() != connectivity.Shutdown {
			return conn, nil
		}
		delete(p.conns, nodeID)
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

// Invalidate removes the cached connection for a node so the next Conn call
// re-dials. The actual Close() is deferred by invalidateGracePeriod so that
// concurrent goroutines holding the same *ClientConn can finish their
// in-flight RPCs before the underlying transport is torn down.
//
// Synchronously closing a shared conn was the source of "client connection
// is closing" cascades: per-RPC error handlers across the cluster forwarders
// (chunk_transferrer, search_forwarder, record_forwarder, …) all call this
// on any failure, and ~50 such call sites racing against each other meant a
// single network blip would propagate as a flurry of mid-RPC closures.
func (p *PeerConns) Invalidate(nodeID string) {
	p.mu.Lock()
	conn, ok := p.conns[nodeID]
	if ok {
		delete(p.conns, nodeID)
	}
	p.mu.Unlock()

	if !ok {
		return
	}
	go func() {
		time.Sleep(invalidateGracePeriod)
		_ = conn.Close()
	}()
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

// PeerIDs returns the node IDs of all peers (excluding self) as strings.
func (p *PeerConns) PeerIDs() []string {
	peers, err := p.Peers()
	if err != nil {
		return nil
	}
	ids := make([]string, len(peers))
	for i, srv := range peers {
		ids[i] = string(srv.ID)
	}
	return ids
}

// Reset swaps the raft pointer and drops all cached connections, forcing
// fresh dials on the next Conn call. The actual Close() of the dropped
// connections is deferred by invalidateGracePeriod so concurrent in-flight
// RPCs can drain — same rationale as Invalidate.
//
// Components holding a *PeerConns reference (Broadcaster, RecordForwarder,
// SearchForwarder) automatically use the new Raft instance without recreation.
func (p *PeerConns) Reset(r *hraft.Raft) {
	p.mu.Lock()
	p.raft = r
	dropped := make([]*grpc.ClientConn, 0, len(p.conns))
	for id, conn := range p.conns {
		dropped = append(dropped, conn)
		delete(p.conns, id)
	}
	p.mu.Unlock()

	if len(dropped) == 0 {
		return
	}
	go func() {
		time.Sleep(invalidateGracePeriod)
		for _, conn := range dropped {
			_ = conn.Close()
		}
	}()
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
