package relp

import (
	"cmp"

	"gastrolog/internal/orchestrator"
)

// ListenAddrs returns the network address this RELP ingester would bind to.
func ListenAddrs(params map[string]string) []orchestrator.ListenAddr {
	return []orchestrator.ListenAddr{
		{Network: "tcp", Address: cmp.Or(params["addr"], ":2514")},
	}
}
