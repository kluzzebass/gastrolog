package routing

import (
	"context"
	"fmt"

	"gastrolog/internal/cluster"
)

// Forwarder sends requests to remote cluster nodes via the ForwardRPC
// bidirectional gRPC stream. It uses the shared PeerConns pool.
type Forwarder struct {
	peers *cluster.PeerConnManager
}

// NewForwarder creates a Forwarder using the shared PeerConns pool.
// Returns nil if peers is nil (single-node mode).
func NewForwarder(peers *cluster.PeerConnManager) *Forwarder {
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
