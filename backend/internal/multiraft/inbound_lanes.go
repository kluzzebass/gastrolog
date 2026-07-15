package multiraft

import (
	"net"
	"sync"
)

// InboundLaneRegistry holds virtual listeners for per-group raft gRPC stacks.
// The cluster port demuxer delivers TLS connections here by group ID after
// parsing the ClientHello SNI (see LaneSNI / GroupIDFromLaneSNI).
type InboundLaneRegistry struct {
	addr  net.Addr
	mu    sync.Mutex
	lanes map[string]*inboundLaneListener
}

type inboundLaneListener struct {
	ch     chan net.Conn
	closed chan struct{}
	addr   net.Addr
}

// NewInboundLaneRegistry creates a registry for per-group raft listeners.
func NewInboundLaneRegistry(addr net.Addr) *InboundLaneRegistry {
	return &InboundLaneRegistry{
		addr:  addr,
		lanes: make(map[string]*inboundLaneListener),
	}
}

// Listener returns the net.Listener for groupID, creating it if needed.
func (r *InboundLaneRegistry) Listener(groupID string) net.Listener {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.lanes[groupID]; ok {
		return l
	}
	l := &inboundLaneListener{
		ch:     make(chan net.Conn, 16),
		closed: make(chan struct{}),
		addr:   r.addr,
	}
	r.lanes[groupID] = l
	return l
}

// Deliver routes conn to the listener for groupID. Returns false when no
// listener has been registered for that group.
func (r *InboundLaneRegistry) Deliver(groupID string, conn net.Conn) bool {
	r.mu.Lock()
	l, ok := r.lanes[groupID]
	r.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case l.ch <- conn:
		return true
	case <-l.closed:
		_ = conn.Close()
		return false
	}
}

// Remove closes and drops the listener for groupID.
func (r *InboundLaneRegistry) Remove(groupID string) {
	r.mu.Lock()
	l, ok := r.lanes[groupID]
	if ok {
		delete(r.lanes, groupID)
	}
	r.mu.Unlock()
	if ok {
		close(l.closed)
	}
}

// Close shuts down all lane listeners.
func (r *InboundLaneRegistry) Close() {
	r.mu.Lock()
	lanes := r.lanes
	r.lanes = make(map[string]*inboundLaneListener)
	r.mu.Unlock()
	for _, l := range lanes {
		close(l.closed)
	}
}

func (l *inboundLaneListener) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-l.ch:
		if !ok {
			return nil, net.ErrClosed
		}
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *inboundLaneListener) Close() error {
	select {
	case <-l.closed:
		return net.ErrClosed
	default:
		close(l.closed)
		return nil
	}
}

func (l *inboundLaneListener) Addr() net.Addr { return l.addr }
