package relp

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"

	"gastrolog/internal/cert"
)

// ParamDefaults returns the default parameter values for a RELP ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"addr": ":2514",
	}
}

// NewFactory returns an IngesterFactory for RELP ingesters.
// The cert manager is used to resolve TLS certificate names.
func NewFactory(certMgr *cert.Manager) ingestion.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (ingestion.Ingester, error) {
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
