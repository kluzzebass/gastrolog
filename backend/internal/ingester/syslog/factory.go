package syslog

import (
	"errors"
	"gastrolog/internal/cert"
	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"
)

// ParamDefaults returns the default parameter values for a syslog ingester.
func ParamDefaults() map[string]string {
	return map[string]string{}
}

// NewFactory returns a IngesterFactory for syslog ingesters.
// The cert manager resolves TLS certificate names; TLS covers the TCP
// listener only (RFC 5425) — UDP syslog has no TLS.
func NewFactory(certMgr *cert.Manager) ingestion.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (ingestion.Ingester, error) {
		udpAddr := params["udp_addr"]
		tcpAddr := params["tcp_addr"]

		if udpAddr == "" && tcpAddr == "" {
			return nil, errors.New("syslog ingester: at least one of udp_addr or tcp_addr is required")
		}

		tlsCfg, err := ingesttls.Server("syslog", params, certMgr)
		if err != nil {
			return nil, err
		}
		return New(Config{
			ID:        id.String(),
			UDPAddr:   udpAddr,
			TCPAddr:   tcpAddr,
			TLSConfig: tlsCfg,
			Logger:    logger,
		}), nil
	}
}
