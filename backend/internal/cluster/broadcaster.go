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

	mu      sync.Mutex
	failed  map[string]bool                   // true = peer is unreachable (suppress repeated logs)
	sent    map[metricsKey]int64              // per (peer, type) success counter
	errored map[metricsKey]int64              // per (peer, type) failure counter
}

// metricsKey identifies a (peer, message-type) bucket for counters.
type metricsKey struct {
	peer string
	typ  string
}

// NewBroadcaster creates a Broadcaster that uses the shared PeerConns pool.
func NewBroadcaster(peers *PeerConns, logger *slog.Logger) *Broadcaster {
	return &Broadcaster{
		peers:   peers,
		logger:  logger,
		failed:  make(map[string]bool),
		sent:    make(map[metricsKey]int64),
		errored: make(map[metricsKey]int64),
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

	typ := PayloadType(msg)
	req := &gastrologv1.BroadcastRequest{Message: msg}
	for _, p := range peers {
		id := string(p.ID)
		conn, err := b.peers.Conn(id)
		if err != nil {
			b.logPeerError(id, "dial peer", err)
			b.recordError(id, typ)
			continue
		}
		if err := b.sendOne(ctx, conn, req); err != nil {
			b.logPeerError(id, "send", err)
			b.recordError(id, typ)
			b.peers.Invalidate(id)
		} else {
			b.clearPeerError(id)
			b.recordSent(id, typ)
		}
	}
}

// recordSent bumps the success counter for (peer, type).
func (b *Broadcaster) recordSent(peer, typ string) {
	b.mu.Lock()
	b.sent[metricsKey{peer: peer, typ: typ}]++
	b.mu.Unlock()
}

// recordError bumps the error counter for (peer, type).
func (b *Broadcaster) recordError(peer, typ string) {
	b.mu.Lock()
	b.errored[metricsKey{peer: peer, typ: typ}]++
	b.mu.Unlock()
}

// BroadcastCounter is a (peer, type, value) triplet used by the metrics
// writer. Exposed for consumers that want to emit Prometheus text.
type BroadcastCounter struct {
	Peer  string
	Type  string
	Value int64
}

// MetricsSnapshot returns the current send / error counters for all (peer,
// type) combinations that have seen traffic. Stable ordering left to the
// caller; the underlying maps are not sorted here.
func (b *Broadcaster) MetricsSnapshot() (sent, errors []BroadcastCounter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sent = make([]BroadcastCounter, 0, len(b.sent))
	for k, v := range b.sent {
		sent = append(sent, BroadcastCounter{Peer: k.peer, Type: k.typ, Value: v})
	}
	errors = make([]BroadcastCounter, 0, len(b.errored))
	for k, v := range b.errored {
		errors = append(errors, BroadcastCounter{Peer: k.peer, Type: k.typ, Value: v})
	}
	return sent, errors
}

// PayloadType returns the broadcast payload's oneof variant as a stable
// label ("node_stats", "node_jobs", or "unknown"). Used for Prometheus
// metrics labels and log fields.
func PayloadType(msg *gastrologv1.BroadcastMessage) string {
	if msg == nil {
		return "unknown"
	}
	switch msg.Payload.(type) {
	case *gastrologv1.BroadcastMessage_NodeStats:
		return "node_stats"
	case *gastrologv1.BroadcastMessage_NodeJobs:
		return "node_jobs"
	default:
		return "unknown"
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
