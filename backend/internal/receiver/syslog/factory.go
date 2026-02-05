package syslog

import (
	"log/slog"

	"gastrolog/internal/orchestrator"
)

// NewFactory returns a ReceiverFactory for syslog receivers.
func NewFactory() orchestrator.ReceiverFactory {
	return func(params map[string]string, logger *slog.Logger) (orchestrator.Receiver, error) {
		udpAddr := params["udp_addr"]
		tcpAddr := params["tcp_addr"]

		// Default to UDP on 514 if nothing specified.
		if udpAddr == "" && tcpAddr == "" {
			udpAddr = ":514"
		}

		return New(Config{
			UDPAddr: udpAddr,
			TCPAddr: tcpAddr,
			Logger:  logger,
		}), nil
	}
}
