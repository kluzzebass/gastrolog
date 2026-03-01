package cluster

import (
	"context"
	"log/slog"
	"sync"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
)

// Broadcaster fans out BroadcastMessages to all cluster peers via the
// Broadcast RPC. Unlike Forwarder (leader-only), Broadcaster maintains
// connections to every peer in the Raft configuration.
type Broadcaster struct {
	peers  *PeerConns
	logger *slog.Logger

	mu     sync.Mutex
	failed map[string]bool // true = peer is unreachable (suppress repeated logs)
}

// NewBroadcaster creates a Broadcaster that uses the shared PeerConns pool.
func NewBroadcaster(peers *PeerConns, logger *slog.Logger) *Broadcaster {
	return &Broadcaster{
		peers:  peers,
		logger: logger,
		failed: make(map[string]bool),
	}
}

// Send broadcasts a message to all peers. Best-effort: errors are logged
// but don't block delivery to other peers.
func (b *Broadcaster) Send(ctx context.Context, msg *gastrologv1.BroadcastMessage) {
	peers, err := b.peers.Peers()
	if err != nil {
		b.logger.Debug("broadcast: get peers", "error", err)
		return
	}
	if len(peers) == 0 {
		return
	}

	req := &gastrologv1.BroadcastRequest{Message: msg}
	for _, p := range peers {
		id := string(p.ID)
		conn, err := b.peers.Conn(id)
		if err != nil {
			b.logPeerError(id, "dial peer", err)
			continue
		}
		if err := b.sendOne(ctx, conn, req); err != nil {
			b.logPeerError(id, "send", err)
			b.peers.Invalidate(id)
		} else {
			b.clearPeerError(id)
		}
	}
}

// logPeerError logs the first error for a peer, then suppresses repeats.
func (b *Broadcaster) logPeerError(id string, action string, err error) {
	b.mu.Lock()
	alreadyFailed := b.failed[id]
	b.failed[id] = true
	b.mu.Unlock()

	if !alreadyFailed {
		b.logger.Debug("broadcast: "+action, "peer", id, "error", err)
	}
}

// clearPeerError marks a peer as healthy and logs recovery if it was down.
func (b *Broadcaster) clearPeerError(id string) {
	b.mu.Lock()
	wasFailed := b.failed[id]
	delete(b.failed, id)
	b.mu.Unlock()

	if wasFailed {
		b.logger.Info("broadcast: peer recovered", "peer", id)
	}
}

// sendOne sends a BroadcastRequest to a single peer via the manually-defined
// RPC path (same pattern as ForwardApplyClient).
func (b *Broadcaster) sendOne(ctx context.Context, conn *grpc.ClientConn, req *gastrologv1.BroadcastRequest) error {
	out := &gastrologv1.BroadcastResponse{}
	return conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Broadcast", req, out)
}

// Close is a no-op — connection lifecycle is managed by PeerConns.
func (b *Broadcaster) Close() error { return nil }
