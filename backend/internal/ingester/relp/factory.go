package relp

import (
	"log/slog"

	"gastrolog/internal/orchestrator"
)

// NewFactory returns an IngesterFactory for RELP ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id string, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		addr := params["addr"]
		if addr == "" {
			addr = ":2514" // RELP convention port
		}

		return New(Config{
			ID:     id,
			Addr:   addr,
			Logger: logger,
		}), nil
	}
}
