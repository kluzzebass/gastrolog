package syslog

import "gastrolog/internal/pipeline/ingestion"

// ListenAddrs returns the network addresses this syslog ingester would bind to.
func ListenAddrs(params map[string]string) []ingestion.ListenAddr {
	var addrs []ingestion.ListenAddr
	if a := params["udp_addr"]; a != "" {
		addrs = append(addrs, ingestion.ListenAddr{Network: "udp", Address: a})
	}
	if a := params["tcp_addr"]; a != "" {
		addrs = append(addrs, ingestion.ListenAddr{Network: "tcp", Address: a})
	}
	return addrs
}
