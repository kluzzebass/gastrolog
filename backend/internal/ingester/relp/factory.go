package relp

import (
	"log/slog"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

// NewFactory returns an IngesterFactory for RELP ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		addr := params["addr"]
		if addr == "" {
			addr = ":2514" // RELP convention port
		}

		return New(Config{
			ID:     id.String(),
			Addr:   addr,
			Logger: logger,
		}), nil
	}
}
