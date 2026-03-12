package otlp

import (
	"cmp"

	"gastrolog/internal/orchestrator"
)

// ListenAddrs returns the network addresses this OTLP ingester would bind to.
func ListenAddrs(params map[string]string) []orchestrator.ListenAddr {
	return []orchestrator.ListenAddr{
		{Network: "tcp", Address: cmp.Or(params["grpc_addr"], ":4317")},
		{Network: "tcp", Address: cmp.Or(params["http_addr"], ":4318")},
	}
}
