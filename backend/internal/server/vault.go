package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/system"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
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

// VaultServer implements the VaultService.
type VaultServer struct {
	orch              *orchestrator.Orchestrator
	cfgStore          system.Store
	factories         orchestrator.Factories
	peerStats         PeerVaultStatsProvider
	remoteChunkLister RemoteChunkLister
	localNodeID       string
	logger            *slog.Logger
}

var _ gastrologv1connect.VaultServiceHandler = (*VaultServer)(nil)

// NewVaultServer creates a new VaultServer.
func NewVaultServer(orch *orchestrator.Orchestrator, cfgStore system.Store, factories orchestrator.Factories, peerStats PeerVaultStatsProvider, remoteChunkLister RemoteChunkLister, localNodeID string, logger *slog.Logger) *VaultServer {
	return &VaultServer{
		orch:              orch,
		cfgStore:          cfgStore,
		factories:         factories,
		peerStats:         peerStats,
		remoteChunkLister: remoteChunkLister,
		localNodeID:       localNodeID,
		logger:            logging.Default(logger).With("component", "vault-server"),
	}
}

func (s *VaultServer) now() time.Time { return time.Now() }

// mapVaultError converts orchestrator errors to connect errors.
// ErrVaultNotFound maps to CodeNotFound; everything else to CodeInternal.
func mapVaultError(err error) *connect.Error {
	if errors.Is(err, orchestrator.ErrVaultNotFound) {
		return errNotFound(err)
	}
	return errInternal(err)
}

// parseUUID parses a string into a uuid.UUID, returning a connect error on failure.
func parseUUID(s string) (uuid.UUID, *connect.Error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("invalid ID %q: %w", s, err))
	}
	return id, nil
}
