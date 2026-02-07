package syslog

import (
	"log/slog"

	"gastrolog/internal/orchestrator"
)

// NewFactory returns a IngesterFactory for syslog ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id string, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		udpAddr := params["udp_addr"]
		tcpAddr := params["tcp_addr"]

		// Default to UDP on 514 if nothing specified.
		if udpAddr == "" && tcpAddr == "" {
			udpAddr = ":514"
		}

		return New(Config{
			ID:      id,
			UDPAddr: udpAddr,
			TCPAddr: tcpAddr,
			Logger:  logger,
		}), nil
	}
}
