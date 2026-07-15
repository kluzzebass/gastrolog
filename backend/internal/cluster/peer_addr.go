package cluster

import (
	"net"
	"strconv"
	"strings"
)

// clusterAddrsEquivalent reports whether two cluster listen/advertise addresses
// refer to the same gRPC endpoint. Raft may store ":4586" in configuration
// while LeaderWithID() reports "[::]:4586" for the same listener.
func clusterAddrsEquivalent(a, b string) bool {
	if a == b {
		return true
	}
	pa, okA := parseClusterAddr(a)
	pb, okB := parseClusterAddr(b)
	if !okA || !okB {
		return false
	}
	if pa.port != pb.port {
		return false
	}
	return clusterHostKeysEquivalent(pa.hostKey, pb.hostKey)
}

type parsedClusterAddr struct {
	hostKey string
	port    uint16
}

func parseClusterAddr(raw string) (parsedClusterAddr, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return parsedClusterAddr{}, false
	}

	host, portStr, err := net.SplitHostPort(raw)
	if err != nil {
		// Bare ":4586" from --cluster-addr :4586.
		if p, ok := strings.CutPrefix(raw, ":"); ok {
			portStr = p
			host = ""
		} else {
			return parsedClusterAddr{}, false
		}
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return parsedClusterAddr{}, false
	}

	return parsedClusterAddr{
		hostKey: clusterHostKey(host),
		port:    uint16(port),
	}, true
}

const (
	clusterHostWildcard = "wildcard"
	clusterHostLoopback = "loopback"
)

func clusterHostKey(host string) string {
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	switch strings.ToLower(host) {
	case "", "0.0.0.0", "::", "*":
		return clusterHostWildcard
	case "localhost", "127.0.0.1", "::1":
		return clusterHostLoopback
	default:
		return strings.ToLower(host)
	}
}

func clusterHostKeysEquivalent(a, b string) bool {
	if a == b {
		return true
	}
	// Wildcard bind (:4586, [::]:4586) matches loopback dials and LeaderWithID().
	if a == clusterHostWildcard || b == clusterHostWildcard {
		return true
	}
	if a == clusterHostLoopback && b == clusterHostLoopback {
		return true
	}
	return false
}
