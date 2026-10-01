package http

import (
	"cmp"
	"fmt"
	"gastrolog/internal/cert"
	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"
)

// ParamDefaults returns the default parameter values for an HTTP ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"addr": ":3100",
	}
}

// NewFactory returns a IngesterFactory for HTTP ingesters.
// The cert manager resolves TLS certificate names.
func NewFactory(certMgr *cert.Manager) ingestion.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (ingestion.Ingester, error) {
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

		tlsCfg, err := ingesttls.Server("http", params, certMgr)
		if err != nil {
			return nil, err
		}
		return New(Config{
			ID:        id.String(),
			Addr:      addr,
			TLSConfig: tlsCfg,
			Logger:    logger,
		}), nil
	}
}
