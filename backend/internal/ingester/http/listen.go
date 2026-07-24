package http

import (
	"cmp"
	"gastrolog/internal/pipeline/ingestion"
)

// ListenAddrs returns the network address this HTTP ingester would bind to.
func ListenAddrs(params map[string]string) []ingestion.ListenAddr {
	return []ingestion.ListenAddr{
		{Network: "tcp", Address: cmp.Or(params["addr"], ":3100")},
	}
}
