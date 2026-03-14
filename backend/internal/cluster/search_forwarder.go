package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
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

// ListChunks sends a ForwardListChunks RPC to the given node.
func (sf *SearchForwarder) ListChunks(ctx context.Context, nodeID string, req *gastrologv1.ForwardListChunksRequest) (*gastrologv1.ForwardListChunksResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardListChunksResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardListChunks", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward list chunks to %s: %w", nodeID, err)
	}
	return resp, nil
}

// GetChunk sends a ForwardGetChunk RPC to the given node.
func (sf *SearchForwarder) GetChunk(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetChunkRequest) (*gastrologv1.ForwardGetChunkResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardGetChunkResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardGetChunk", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward get chunk to %s: %w", nodeID, err)
	}
	return resp, nil
}

// GetIndexes sends a ForwardGetIndexes RPC to the given node.
func (sf *SearchForwarder) GetIndexes(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetIndexesRequest) (*gastrologv1.ForwardGetIndexesResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardGetIndexesResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardGetIndexes", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward get indexes to %s: %w", nodeID, err)
	}
	return resp, nil
}

// AnalyzeChunk sends a ForwardAnalyzeChunk RPC to the given node.
func (sf *SearchForwarder) AnalyzeChunk(ctx context.Context, nodeID string, req *gastrologv1.ForwardAnalyzeChunkRequest) (*gastrologv1.ForwardAnalyzeChunkResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardAnalyzeChunkResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardAnalyzeChunk", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward analyze chunk to %s: %w", nodeID, err)
	}
	return resp, nil
}

// ValidateVault sends a ForwardValidateVault RPC to the given node.
func (sf *SearchForwarder) ValidateVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardValidateVaultRequest) (*gastrologv1.ForwardValidateVaultResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardValidateVaultResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardValidateVault", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward validate vault to %s: %w", nodeID, err)
	}
	return resp, nil
}

// SealVault sends a ForwardSealVault RPC to the given node.
func (sf *SearchForwarder) SealVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardSealVaultRequest) (*gastrologv1.ForwardSealVaultResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardSealVaultResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardSealVault", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward seal vault to %s: %w", nodeID, err)
	}
	return resp, nil
}

// ReindexVault sends a ForwardReindexVault RPC to the given node.
func (sf *SearchForwarder) ReindexVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardReindexVaultRequest) (*gastrologv1.ForwardReindexVaultResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardReindexVaultResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardReindexVault", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward reindex vault to %s: %w", nodeID, err)
	}
	return resp, nil
}

// Explain sends a ForwardExplain RPC to the given node and returns the response.
func (sf *SearchForwarder) Explain(ctx context.Context, nodeID string, req *gastrologv1.ForwardExplainRequest) (*gastrologv1.ForwardExplainResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardExplainResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardExplain", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward explain to %s: %w", nodeID, err)
	}
	return resp, nil
}

// Follow opens a server-streaming ForwardFollow RPC to the given node.
// Returns a channel that yields ExportRecords as they arrive from the remote.
// The channel is closed when the stream ends or ctx is cancelled.
func (sf *SearchForwarder) Follow(ctx context.Context, nodeID string, req *gastrologv1.ForwardFollowRequest) (<-chan *gastrologv1.ExportRecord, <-chan error) {
	recCh := make(chan *gastrologv1.ExportRecord, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(recCh)
		defer close(errCh)

		conn, err := sf.peers.Conn(nodeID)
		if err != nil {
			errCh <- fmt.Errorf("dial node %s: %w", nodeID, err)
			return
		}

		stream, err := conn.NewStream(ctx,
			&grpc.StreamDesc{
				StreamName:    "ForwardFollow",
				ServerStreams: true,
			},
			"/gastrolog.v1.ClusterService/ForwardFollow",
		)
		if err != nil {
			sf.peers.Invalidate(nodeID)
			errCh <- fmt.Errorf("open follow stream to %s: %w", nodeID, err)
			return
		}
		if err := stream.SendMsg(req); err != nil {
			sf.peers.Invalidate(nodeID)
			errCh <- fmt.Errorf("send follow request to %s: %w", nodeID, err)
			return
		}
		if err := stream.CloseSend(); err != nil {
			errCh <- fmt.Errorf("close send to %s: %w", nodeID, err)
			return
		}

		for {
			resp := &gastrologv1.ForwardFollowResponse{}
			if err := stream.RecvMsg(resp); err != nil {
				if !errors.Is(err, io.EOF) {
					errCh <- fmt.Errorf("follow stream from %s: %w", nodeID, err)
				}
				return
			}
			for _, rec := range resp.GetRecords() {
				select {
				case recCh <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return recCh, errCh
}

// ExportToVault sends a ForwardExportToVault RPC to the given node.
func (sf *SearchForwarder) ExportToVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardExportToVaultRequest) (*gastrologv1.ForwardExportToVaultResponse, error) {
	conn, err := sf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}
	resp := &gastrologv1.ForwardExportToVaultResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardExportToVault", req, resp); err != nil {
		sf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("forward export to vault to %s: %w", nodeID, err)
	}
	return resp, nil
}

// Close is a no-op — connection lifecycle is managed by PeerConns.
func (sf *SearchForwarder) Close() error { return nil }
