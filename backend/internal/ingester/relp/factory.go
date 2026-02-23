package relp

import (
	"log/slog"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for a RELP ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"addr": ":2514",
	}
}

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
