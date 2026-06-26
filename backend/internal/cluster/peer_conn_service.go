package cluster

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

// InvokeService runs a unary ClusterService RPC on the service lane.
func (m *PeerConnManager) InvokeService(ctx context.Context, peerNodeID, purpose, method string, req, resp any) error {
	return m.UseServiceCtx(ctx, peerNodeID, purpose, func(ctx context.Context, conn *grpc.ClientConn) error {
		if err := conn.Invoke(ctx, method, req, resp); err != nil {
			return err
		}
		return nil
	})
}

// OpenServiceStream acquires a service-lane connection and opens a client
// stream. The caller must call handle.Release() when the stream is finished.
func (m *PeerConnManager) OpenServiceStream(ctx context.Context, peerNodeID, purpose string, desc *grpc.StreamDesc, method string) (PeerConnHandle, grpc.ClientStream, error) {
	h, err := m.AcquireService(peerNodeID, purpose)
	if err != nil {
		return nil, nil, fmt.Errorf("dial node %s: %w", peerNodeID, err)
	}
	stream, err := h.GRPC().NewStream(ctx, desc, method)
	if err != nil {
		h.Invalidate(err)
		h.Release()
		return nil, nil, err
	}
	return h, stream, nil
}
