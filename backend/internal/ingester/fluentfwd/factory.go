package fluentfwd

import (
	"cmp"
	"fmt"
	"gastrolog/internal/cert"
	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"
)

// ParamDefaults returns the default parameter values for a Fluent Forward ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"addr": ":24224",
	}
}

// NewFactory returns an IngesterFactory for Fluent Forward ingesters.
// The cert manager resolves TLS certificate names.
func NewFactory(certMgr *cert.Manager) ingestion.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (ingestion.Ingester, error) {
		addr := cmp.Or(params["addr"], ":24224")

		// Validate addr format.
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

		tlsCfg, err := ingesttls.Server("fluentfwd", params, certMgr)
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
