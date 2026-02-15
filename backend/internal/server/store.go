package server

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/config"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// StoreServer implements the StoreService.
type StoreServer struct {
	orch      *orchestrator.Orchestrator
	cfgStore  config.Store
	factories orchestrator.Factories
	logger    *slog.Logger
}

var _ gastrologv1connect.StoreServiceHandler = (*StoreServer)(nil)

// NewStoreServer creates a new StoreServer.
func NewStoreServer(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, logger *slog.Logger) *StoreServer {
	return &StoreServer{
		orch:      orch,
		cfgStore:  cfgStore,
		factories: factories,
		logger:    logging.Default(logger).With("component", "store-server"),
	}
}

func (s *StoreServer) now() time.Time { return time.Now() }

// mapStoreError converts orchestrator errors to connect errors.
// ErrStoreNotFound maps to CodeNotFound; everything else to CodeInternal.
func mapStoreError(err error) *connect.Error {
	if errors.Is(err, orchestrator.ErrStoreNotFound) {
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
