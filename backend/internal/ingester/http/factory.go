package http

import (
	"cmp"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for an HTTP ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"addr": ":3100",
	}
}

// NewFactory returns a IngesterFactory for HTTP ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		addr := cmp.Or(params["addr"], ":3100") // Loki's default port

		// Validate addr format (basic check).
		if addr[0] != ':' && addr[0] != '[' {
			// Check for host:port format.
			hasColon := false
			for _, c := range addr {
				if c == ':' {
					hasColon = true
					break
				}
			}
			if !hasColon {
				return nil, fmt.Errorf("invalid addr %q: must be :port or host:port", addr)
			}
		}

		return New(Config{
			ID:     id.String(),
			Addr:   addr,
			Logger: logger,
		}), nil
	}
}
