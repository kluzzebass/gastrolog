package otlp

import (
	"cmp"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for an OTLP ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"http_addr": ":4318",
		"grpc_addr": ":4317",
	}
}

// NewFactory returns an IngesterFactory for OTLP ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		httpAddr := cmp.Or(params["http_addr"], ":4318")
		grpcAddr := cmp.Or(params["grpc_addr"], ":4317")

		// Validate addr formats.
		for _, addr := range []string{httpAddr, grpcAddr} {
			if addr[0] != ':' && addr[0] != '[' {
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
		}

		return New(Config{
			ID:       id.String(),
			HTTPAddr: httpAddr,
			GRPCAddr: grpcAddr,
			Logger:   logger,
		}), nil
	}
}
