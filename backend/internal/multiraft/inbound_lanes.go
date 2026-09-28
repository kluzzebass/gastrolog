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
	ch        chan net.Conn
	closed    chan struct{}
	closeOnce sync.Once
	addr      net.Addr
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

// Remove closes and drops the listener for groupID. The listener may
// already have closed itself — a group's gRPC server closes its listener on
// stop, and DestroyGroup then removes the lane — so this goes through the
// listener's guarded Close rather than closing the channel directly.
func (r *InboundLaneRegistry) Remove(groupID string) {
	r.mu.Lock()
	l, ok := r.lanes[groupID]
	if ok {
		delete(r.lanes, groupID)
	}
	r.mu.Unlock()
	if ok {
		_ = l.Close()
	}
}

// Close shuts down all lane listeners.
func (r *InboundLaneRegistry) Close() {
	r.mu.Lock()
	lanes := r.lanes
	r.lanes = make(map[string]*inboundLaneListener)
	r.mu.Unlock()
	for _, l := range lanes {
		_ = l.Close()
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

// Close is safe to call from every shutdown path concurrently: the group's
// gRPC server closes its listener on stop, and the registry closes it again
// on Remove. A select-default guard here is check-then-act — two concurrent
// closers can both pass it — so the close funnels through a Once.
func (l *inboundLaneListener) Close() error {
	err := net.ErrClosed
	l.closeOnce.Do(func() {
		close(l.closed)
		err = nil
	})
	return err
}

func (l *inboundLaneListener) Addr() net.Addr { return l.addr }
