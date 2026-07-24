package fluentfwd

import (
	"cmp"
	"gastrolog/internal/pipeline/ingestion"
)

// ListenAddrs returns the network address this Fluent Forward ingester would bind to.
func ListenAddrs(params map[string]string) []ingestion.ListenAddr {
	return []ingestion.ListenAddr{
		{Network: "tcp", Address: cmp.Or(params["addr"], ":24224")},
	}
}
