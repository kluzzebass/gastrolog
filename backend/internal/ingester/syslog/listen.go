package syslog

import "gastrolog/internal/orchestrator"

// ListenAddrs returns the network addresses this syslog ingester would bind to.
func ListenAddrs(params map[string]string) []orchestrator.ListenAddr {
	var addrs []orchestrator.ListenAddr
	if a := params["udp_addr"]; a != "" {
		addrs = append(addrs, orchestrator.ListenAddr{Network: "udp", Address: a})
	}
	if a := params["tcp_addr"]; a != "" {
		addrs = append(addrs, orchestrator.ListenAddr{Network: "tcp", Address: a})
	}
	return addrs
}
