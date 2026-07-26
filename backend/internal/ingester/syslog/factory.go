package syslog

import (
	"errors"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"
)

// ParamDefaults returns the default parameter values for a syslog ingester.
func ParamDefaults() map[string]string {
	return map[string]string{}
}

// NewFactory returns a IngesterFactory for syslog ingesters.
func NewFactory() ingestion.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (ingestion.Ingester, error) {
		udpAddr := params["udp_addr"]
		tcpAddr := params["tcp_addr"]

		if udpAddr == "" && tcpAddr == "" {
			return nil, errors.New("syslog ingester: at least one of udp_addr or tcp_addr is required")
		}

		return New(Config{
			ID:      id.String(),
			UDPAddr: udpAddr,
			TCPAddr: tcpAddr,
			Logger:  logger,
		}), nil
	}
}
