package routing

import (
	"context"
	"fmt"
	"io"

	"gastrolog/internal/cluster"
)

// Forwarder sends requests to remote cluster nodes via the ForwardRPC
// bidirectional gRPC stream. It uses the shared PeerConns pool.
type Forwarder struct {
	peers *cluster.PeerConns
}

// NewForwarder creates a Forwarder using the shared PeerConns pool.
// Returns nil if peers is nil (single-node mode).
func NewForwarder(peers *cluster.PeerConns) *Forwarder {
	if peers == nil {
		return nil
	}
	return &Forwarder{peers: peers}
}

// ForwardUnary sends a serialized request to a remote node and returns the
// serialized response. Used by the routing interceptor for unary RPCs.
func (f *Forwarder) ForwardUnary(ctx context.Context, nodeID, procedure string, reqPayload []byte) ([]byte, error) {
	payload, errCode, errMsg, err := cluster.ForwardRPC(ctx, f.peers, nodeID, procedure, reqPayload)
	if err != nil {
		return nil, fmt.Errorf("forward %s to %s: %w", procedure, nodeID, err)
	}
	if errCode != 0 {
		return nil, &RemoteError{Code: errCode, Message: errMsg}
	}
	return payload, nil
}

// StreamReceiver yields response frames from a server-streaming ForwardRPC.
type StreamReceiver struct {
	sender *cluster.ForwardRPCStreamSender
}

// Recv reads the next serialized response payload. Returns io.EOF at stream end.
func (r *StreamReceiver) Recv() ([]byte, error) {
	frame, err := r.sender.Recv()
	if err != nil {
		return nil, err
	}
	if frame.ErrorCode != 0 {
		return nil, &RemoteError{Code: frame.ErrorCode, Message: frame.ErrorMessage}
	}
	return frame.Payload, nil
}

// Close signals we are done reading.
func (r *StreamReceiver) Close() {
	r.sender.Close()
}

// ForwardServerStream opens a server-streaming ForwardRPC to a remote node.
// Returns a StreamReceiver that yields serialized response payloads.
func (f *Forwarder) ForwardServerStream(ctx context.Context, nodeID, procedure string, reqPayload []byte) (*StreamReceiver, error) {
	sender, err := cluster.ForwardRPCStream(ctx, f.peers, nodeID, procedure, reqPayload)
	if err != nil {
		return nil, fmt.Errorf("forward stream %s to %s: %w", procedure, nodeID, err)
	}
	return &StreamReceiver{sender: sender}, nil
}

// RemoteError represents an error returned by a remote node's handler.
type RemoteError struct {
	Code    uint32
	Message string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote error (code=%d): %s", e.Code, e.Message)
}

// VaultOwnerResolver looks up which node owns a vault by ID.
type VaultOwnerResolver interface {
	// ResolveVaultOwner returns the node ID that owns the vault, or empty
	// string if the vault is not found or has no assigned node.
	ResolveVaultOwner(ctx context.Context, vaultID string) string
}

// Ensure StreamReceiver is usable in tests.
var _ io.Closer = (*closableStreamReceiver)(nil)

type closableStreamReceiver struct{ *StreamReceiver }

func (c *closableStreamReceiver) Close() error {
	c.StreamReceiver.Close()
	return nil
}
