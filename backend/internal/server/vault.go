package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/config"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// PeerVaultStatsProvider looks up vault stats from cluster peer broadcasts.
// Implemented by cluster.PeerState; nil in single-node mode.
type PeerVaultStatsProvider interface {
	FindVaultStats(vaultID string) *apiv1.VaultStats
}

// RemoteVaultForwarder forwards vault RPCs to remote cluster nodes.
// Implemented by cluster.SearchForwarder; nil in single-node mode.
type RemoteVaultForwarder interface {
	ListChunks(ctx context.Context, nodeID string, req *apiv1.ForwardListChunksRequest) (*apiv1.ForwardListChunksResponse, error)
	GetChunk(ctx context.Context, nodeID string, req *apiv1.ForwardGetChunkRequest) (*apiv1.ForwardGetChunkResponse, error)
	GetIndexes(ctx context.Context, nodeID string, req *apiv1.ForwardGetIndexesRequest) (*apiv1.ForwardGetIndexesResponse, error)
	AnalyzeChunk(ctx context.Context, nodeID string, req *apiv1.ForwardAnalyzeChunkRequest) (*apiv1.ForwardAnalyzeChunkResponse, error)
	ValidateVault(ctx context.Context, nodeID string, req *apiv1.ForwardValidateVaultRequest) (*apiv1.ForwardValidateVaultResponse, error)
	SealVault(ctx context.Context, nodeID string, req *apiv1.ForwardSealVaultRequest) (*apiv1.ForwardSealVaultResponse, error)
	ReindexVault(ctx context.Context, nodeID string, req *apiv1.ForwardReindexVaultRequest) (*apiv1.ForwardReindexVaultResponse, error)
}

// VaultServer implements the VaultService.
type VaultServer struct {
	orch        *orchestrator.Orchestrator
	cfgStore    config.Store
	factories   orchestrator.Factories
	peerStats   PeerVaultStatsProvider
	remote      RemoteVaultForwarder
	localNodeID string
	logger      *slog.Logger
}

var _ gastrologv1connect.VaultServiceHandler = (*VaultServer)(nil)

// NewVaultServer creates a new VaultServer.
func NewVaultServer(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, peerStats PeerVaultStatsProvider, remote RemoteVaultForwarder, localNodeID string, logger *slog.Logger) *VaultServer {
	return &VaultServer{
		orch:        orch,
		cfgStore:    cfgStore,
		factories:   factories,
		peerStats:   peerStats,
		remote:      remote,
		localNodeID: localNodeID,
		logger:      logging.Default(logger).With("component", "vault-server"),
	}
}

// remoteNodeForVault returns the owning node ID if the vault is remote.
// Returns "" if the vault is local or not found.
func (s *VaultServer) remoteNodeForVault(ctx context.Context, vaultID uuid.UUID) string {
	if slices.Contains(s.orch.ListVaults(), vaultID) {
		return ""
	}
	if s.cfgStore == nil {
		return ""
	}
	allCfg, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return ""
	}
	for _, vc := range allCfg {
		if vc.ID == vaultID {
			return vc.NodeID
		}
	}
	return ""
}

func (s *VaultServer) now() time.Time { return time.Now() }

// mapVaultError converts orchestrator errors to connect errors.
// ErrVaultNotFound maps to CodeNotFound; everything else to CodeInternal.
func mapVaultError(err error) *connect.Error {
	if errors.Is(err, orchestrator.ErrVaultNotFound) {
		return connect.NewError(connect.CodeNotFound, err)
	}
	return connect.NewError(connect.CodeInternal, err)
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
