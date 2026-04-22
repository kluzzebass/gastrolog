package relp

import (
	"gastrolog/internal/glid"
	"log/slog"

	"gastrolog/internal/cert"
	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for a RELP ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"addr": ":2514",
	}
}

// NewFactory returns an IngesterFactory for RELP ingesters.
// The cert manager is used to resolve TLS certificate names.
func NewFactory(certMgr *cert.Manager) orchestrator.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		addr := params["addr"]
		if addr == "" {
			addr = ":2514" // RELP convention port
		}

		tlsCfg, err := BuildTLSConfig(params, certMgr)
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
