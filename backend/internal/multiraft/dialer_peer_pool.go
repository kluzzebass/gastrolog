package multiraft

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialerPeerPool resolves outbound connections through a static address→dialer
// map. Used by in-process multiraft tests (bufconn); production nodes use
// cluster.PeerConns via SetPeerConnPool.
type DialerPeerPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	dial  func(context.Context, string) (net.Conn, error)
}

// NewDialerPeerPool creates a peer pool that dials via dialers keyed by target
// address string.
func NewDialerPeerPool(dialers map[string]func(context.Context, string) (net.Conn, error)) *DialerPeerPool {
	return &DialerPeerPool{
		conns: make(map[string]*grpc.ClientConn),
		dial:  dialForMap(dialers),
	}
}

// NewSimpleDialerPeerPool is a convenience wrapper for bufconn tests that use
// dialers returning net.Conn without inspecting the address.
func NewSimpleDialerPeerPool(dialers map[string]func() (net.Conn, error)) *DialerPeerPool {
	mapped := make(map[string]func(context.Context, string) (net.Conn, error), len(dialers))
	for addr, dial := range dialers {
		d := dial
		mapped[addr] = func(_ context.Context, _ string) (net.Conn, error) {
			return d()
		}
	}
	return NewDialerPeerPool(mapped)
}

func dialForMap(dialers map[string]func(context.Context, string) (net.Conn, error)) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		d, ok := dialers[addr]
		if !ok {
			return nil, fmt.Errorf("multiraft test pool: no dialer for %q", addr)
		}
		return d(ctx, addr)
	}
}

func (p *DialerPeerPool) ConnForAddress(addr raft.ServerAddress) (*grpc.ClientConn, error) {
	key := string(addr)
	p.mu.Lock()
	if conn, ok := p.conns[key]; ok {
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	conn, err := grpc.NewClient("passthrough:///"+key,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(p.dial),
	)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if existing, ok := p.conns[key]; ok {
		p.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	p.conns[key] = conn
	p.mu.Unlock()
	return conn, nil
}

// Close tears down cached test connections.
func (p *DialerPeerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, conn := range p.conns {
		_ = conn.Close()
		delete(p.conns, k)
	}
}
