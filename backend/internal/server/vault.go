package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
)

// PeerVaultStatsProvider looks up vault stats from cluster peer broadcasts.
// Implemented by cluster.PeerState; nil in single-node mode.
type PeerVaultStatsProvider interface {
	FindVaultStats(vaultID string) *apiv1.VaultStats
}

// RemoteChunkLister lists chunks from a remote node's vault.
type RemoteChunkLister interface {
	ListChunks(ctx context.Context, nodeID string, req *apiv1.ForwardListChunksRequest) (*apiv1.ForwardListChunksResponse, error)
}

// RemoteIndexer queries chunk index information on a remote node.
// Used by GetIndexes when the chunk has migrated to a tier this node
// doesn't host. See gastrolog-3570f.
type RemoteIndexer interface {
	GetIndexes(ctx context.Context, nodeID string, req *apiv1.ForwardGetIndexesRequest) (*apiv1.ForwardGetIndexesResponse, error)
}

// VaultServer implements the VaultService.
type VaultServer struct {
	orch              *orchestrator.Orchestrator
	cfgStore          system.Store
	factories         orchestrator.Factories
	peerStats         PeerVaultStatsProvider
	remoteChunkLister RemoteChunkLister
	remoteIndexer     RemoteIndexer
	localNodeID       string
	logger            *slog.Logger
}

var _ gastrologv1connect.VaultServiceHandler = (*VaultServer)(nil)

// NewVaultServer creates a new VaultServer.
func NewVaultServer(orch *orchestrator.Orchestrator, cfgStore system.Store, factories orchestrator.Factories, peerStats PeerVaultStatsProvider, remoteChunkLister RemoteChunkLister, remoteIndexer RemoteIndexer, localNodeID string, logger *slog.Logger) *VaultServer {
	return &VaultServer{
		orch:              orch,
		cfgStore:          cfgStore,
		factories:         factories,
		peerStats:         peerStats,
		remoteChunkLister: remoteChunkLister,
		remoteIndexer:     remoteIndexer,
		localNodeID:       localNodeID,
		logger:            logging.Default(logger).With("component", "vault-server"),
	}
}

func (s *VaultServer) now() time.Time { return time.Now() }

// mapVaultError converts orchestrator errors to connect errors.
// ErrVaultNotFound maps to CodeNotFound; ErrVaultNotReady to Unavailable;
// everything else to CodeInternal.
func mapVaultError(err error) *connect.Error {
	if errors.Is(err, orchestrator.ErrVaultNotFound) {
		return errNotFound(err)
	}
	if errors.Is(err, orchestrator.ErrVaultNotReady) {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return errInternal(err)
}

// parseUUID parses a string into a glid.GLID, returning a connect error on failure.
func parseUUID(s string) (glid.GLID, *connect.Error) {
	id, err := glid.ParseUUID(s)
	if err != nil {
		return glid.Nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("invalid ID %q: %w", s, err))
	}
	return id, nil
}

// parseProtoID converts a proto bytes ID (16-byte raw) into a glid.GLID,
// returning a connect error if the length is wrong.
func parseProtoID(b []byte) (glid.GLID, *connect.Error) {
	if len(b) != glid.Size {
		return glid.Nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("invalid ID: expected %d bytes, got %d", glid.Size, len(b)))
	}
	return glid.FromBytes(b), nil
}

// parseProtoChunkID converts a proto bytes chunk ID into a chunk.ChunkID,
// returning a connect error if the length is wrong.
func parseProtoChunkID(b []byte) (chunk.ChunkID, error) {
	if len(b) != glid.Size {
		return chunk.ChunkID{}, fmt.Errorf("invalid chunk ID: expected %d bytes, got %d", glid.Size, len(b))
	}
	return chunk.ChunkID(glid.FromBytes(b)), nil
}
