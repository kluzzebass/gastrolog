package server

import (
	"errors"
	"fmt"
	"log/slog"
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

// VaultServer implements the VaultService.
type VaultServer struct {
	orch      *orchestrator.Orchestrator
	cfgStore  config.Store
	factories orchestrator.Factories
	peerStats PeerVaultStatsProvider
	logger    *slog.Logger
}

var _ gastrologv1connect.VaultServiceHandler = (*VaultServer)(nil)

// NewVaultServer creates a new VaultServer.
func NewVaultServer(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, peerStats PeerVaultStatsProvider, logger *slog.Logger) *VaultServer {
	return &VaultServer{
		orch:      orch,
		cfgStore:  cfgStore,
		factories: factories,
		peerStats: peerStats,
		logger:    logging.Default(logger).With("component", "vault-server"),
	}
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
