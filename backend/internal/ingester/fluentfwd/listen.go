package fluentfwd

import (
	"cmp"

	"gastrolog/internal/orchestrator"
)

// ListenAddrs returns the network address this Fluent Forward ingester would bind to.
func ListenAddrs(params map[string]string) []orchestrator.ListenAddr {
	return []orchestrator.ListenAddr{
		{Network: "tcp", Address: cmp.Or(params["addr"], ":24224")},
	}
}
