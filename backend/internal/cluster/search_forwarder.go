package cluster

import (
	"context"
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// SearchForwarder sends search requests to remote cluster nodes.
// Unlike RecordForwarder (fire-and-forget batching), this is synchronous
// request-response — the caller blocks until the remote node responds.
type SearchForwarder struct {
	peers *PeerConns
}

// NewSearchForwarder creates a SearchForwarder using the shared PeerConns pool.
func NewSearchForwarder(peers *PeerConns) *SearchForwarder {
	return &SearchForwarder{peers: peers}
}

// Search sends a ForwardSearch RPC to the given node and returns the response.
func (sf *SearchForwarder) Search(ctx context.Context, nodeID string, req *gastrologv1.ForwardSearchRequest) (*gastrologv1.ForwardSearchResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardSearchResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardSearch", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward search to %s: %w", nodeID, err)
	}
	return resp, nil
}

// GetContext sends a ForwardGetContext RPC to the given node.
func (sf *SearchForwarder) GetContext(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardGetContextResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardGetContext", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward get context to %s: %w", nodeID, err)
	}
	return resp, nil
}

// Close is a no-op — connection lifecycle is managed by PeerConns.
func (sf *SearchForwarder) Close() error { return nil }
