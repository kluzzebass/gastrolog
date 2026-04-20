package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// shouldInvalidate reports whether err indicates the underlying gRPC
// connection itself is broken and should be discarded. Application-level
// status codes (Internal, NotFound, InvalidArgument, …), caller cancellation
// (Canceled, DeadlineExceeded), and raw io.EOF do NOT signal a dead conn —
// gRPC's own state machine will move the conn into TRANSIENT_FAILURE if the
// transport is genuinely broken, and tearing the shared conn down on every
// app-level RPC error cascades into killing every other consumer's stream.
//
// Only Unavailable reliably means "transport unavailable, retry on a fresh
// conn".
func shouldInvalidate(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return status.Code(err) == codes.Unavailable
}

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
	byteMetrics *PeerByteMetrics // optional; nil disables per-peer byte tracking

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

// SetByteMetrics attaches a PeerByteMetrics tracker. Must be called before
// any Conn() — connections dialed before this have no stats handler and
// won't be tracked. Pass nil to disable tracking.
func (p *PeerConns) SetByteMetrics(m *PeerByteMetrics) {
	p.mu.Lock()
	p.byteMetrics = m
	p.mu.Unlock()
}

// attachNodeIDUnaryInterceptor stamps the local node ID into outgoing
// metadata on every unary RPC. Server-side stats handler reads this to
// attribute inbound bytes to the right peer.
func (p *PeerConns) attachNodeIDUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(metadata.AppendToOutgoingContext(ctx, NodeIDMetadataKey, p.nodeID), method, req, reply, cc, opts...)
}

// attachNodeIDStreamInterceptor stamps the local node ID into outgoing
// metadata on every stream RPC.
func (p *PeerConns) attachNodeIDStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(metadata.AppendToOutgoingContext(ctx, NodeIDMetadataKey, p.nodeID), desc, cc, method, opts...)
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
			// #region agent log
			debugLog4vz40("cluster/peer_conns.go:Conn",
				"cache hit",
				map[string]any{
					"hypothesisId": "A-conn-churn",
					"node":         nodeID,
					"state":        conn.GetState().String(),
				})
			// #endregion
			return conn, nil
		}
		// #region agent log
		debugLog4vz40("cluster/peer_conns.go:Conn",
			"cache hit but conn is SHUTDOWN — evicting and redialing",
			map[string]any{
				"hypothesisId": "A-conn-churn",
				"node":         nodeID,
			})
		// #endregion
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

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  500 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
		}),
		grpc.WithUnaryInterceptor(p.attachNodeIDUnaryInterceptor),
		grpc.WithStreamInterceptor(p.attachNodeIDStreamInterceptor),
	}
	if p.byteMetrics != nil {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(newClientStatsHandler(nodeID, p.byteMetrics)))
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial node %s at %s: %w", nodeID, addr, err)
	}
	// #region agent log
	debugLog4vz40("cluster/peer_conns.go:Conn",
		"fresh dial",
		map[string]any{
			"hypothesisId": "A-conn-churn",
			"node":         nodeID,
			"addr":         addr,
		})
	// #endregion
	p.conns[nodeID] = conn
	return conn, nil
}

// Invalidate drops the cached connection for a node so the next Conn call
// re-dials, but ONLY when err signals an actual transport-level failure
// (see shouldInvalidate). Application-level RPC errors (Internal, NotFound,
// InvalidArgument, …), caller cancellations (Canceled, DeadlineExceeded),
// and raw io.EOF are no-ops — gRPC's own state machine already handles real
// transport breakdowns, and tearing the shared conn down on every app-level
// error cascades into killing every other consumer's stream.
//
// The actual Close() is deferred by invalidateGracePeriod so that concurrent
// goroutines holding the same *ClientConn can finish their in-flight RPCs
// before the underlying transport is torn down.
//
// Synchronously closing a shared conn on any error was the source of
// "client connection is closing" cascades: per-RPC error handlers across
// the cluster forwarders (chunk_transferrer, search_forwarder,
// record_forwarder, …) all called this on any failure, and ~50 such call
// sites racing against each other meant a single application error would
// propagate as a flurry of mid-RPC closures.
func (p *PeerConns) Invalidate(nodeID string, err error) {
	if !shouldInvalidate(err) {
		// #region agent log
		debugLog4vz40("cluster/peer_conns.go:Invalidate",
			"skipped — err is not a transport failure",
			map[string]any{
				"hypothesisId": "HA1-invalidate-storm",
				"node":         nodeID,
				"err":          fmt.Sprint(err),
				"errType":      fmt.Sprintf("%T", err),
				"grpcCode":     status.Code(err).String(),
				"runId":        "post-fix",
			})
		// #endregion
		return
	}
	// #region agent log
	var caller string
	if _, file, line, ok := runtime.Caller(1); ok {
		caller = fmt.Sprintf("%s:%d", file, line)
	}
	// #endregion
	p.mu.Lock()
	conn, ok := p.conns[nodeID]
	if ok {
		delete(p.conns, nodeID)
	}
	p.mu.Unlock()

	// #region agent log
	debugLog4vz40("cluster/peer_conns.go:Invalidate",
		"invalidate called",
		map[string]any{
			"hypothesisId":  "HA1-invalidate-storm",
			"node":          nodeID,
			"caller":        caller,
			"hadCachedConn": ok,
			"grpcCode":      status.Code(err).String(),
			"runId":         "post-fix",
		})
	// #endregion
	if !ok {
		return
	}
	go func() {
		time.Sleep(invalidateGracePeriod)
		// #region agent log
		debugLog4vz40("cluster/peer_conns.go:Invalidate",
			"deferred close firing — torn conn being closed",
			map[string]any{
				"hypothesisId": "HA1-invalidate-storm",
				"node":         nodeID,
				"caller":       caller,
			})
		// #endregion
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
	// #region agent log
	var caller string
	if _, file, line, ok := runtime.Caller(1); ok {
		caller = fmt.Sprintf("%s:%d", file, line)
	}
	// #endregion
	p.mu.Lock()
	p.raft = r
	dropped := make([]*grpc.ClientConn, 0, len(p.conns))
	for id, conn := range p.conns {
		dropped = append(dropped, conn)
		delete(p.conns, id)
	}
	p.mu.Unlock()

	// #region agent log
	debugLog4vz40("cluster/peer_conns.go:Reset",
		"Reset called — all peer conns dropped",
		map[string]any{
			"hypothesisId": "HA4-reset-cascade",
			"caller":       caller,
			"numDropped":   len(dropped),
		})
	// #endregion
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
