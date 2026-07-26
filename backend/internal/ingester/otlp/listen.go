package otlp

import (
	"cmp"
	"gastrolog/internal/pipeline/ingestion"
)

// ListenAddrs returns the network addresses this OTLP ingester would bind to.
func ListenAddrs(params map[string]string) []ingestion.ListenAddr {
	return []ingestion.ListenAddr{
		{Network: "tcp", Address: cmp.Or(params["grpc_addr"], ":4317")},
		{Network: "tcp", Address: cmp.Or(params["http_addr"], ":4318")},
	}
}
