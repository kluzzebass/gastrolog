package cluster

import (
	"gastrolog/internal/multiraft"
)

// TLS Server Name Indication values demux inbound cluster-port connections
// onto separate gRPC server stacks (service vs per-group raft lanes).
const (
	SNIServiceLane = "gastrolog-cluster"
	// SNIRaftLane is the legacy single-stack raft SNI; inbound demux maps it to
	// the cluster config group. Outbound dials use multiraft.LaneSNI(groupID).
	SNIRaftLane = multiraft.RaftLaneSNIPrefix
)

// LaneSANs are included in cluster node certificates so peers can verify
// lane-specific TLS handshakes after SNI demux. Per-group raft lanes use
// gastrolog-raft.<group> SNI names verified via cluster-CA chain check.
var LaneSANs = []string{SNIServiceLane, SNIRaftLane}

// maxRaftLaneRecvBytes caps inbound message size on the raft gRPC stack.
// Smaller than the service lane (128MB chunk transfers) so heavy
// ClusterService streams cannot compete for receive buffers on the raft path.
const maxRaftLaneRecvBytes = 16 * 1024 * 1024 // 16 MB

func serviceConnKey(nodeID string) string {
	return "svc:" + nodeID
}

func raftConnKey(nodeID, groupID string) string {
	return "raft:" + nodeID + ":" + groupID
}

func isRaftConnKey(key string) bool {
	return len(key) >= 5 && key[:5] == "raft:"
}

func groupIDFromRaftConnKey(key string) (string, bool) {
	if !isRaftConnKey(key) {
		return "", false
	}
	// raft:<nodeID>:<groupID> — groupID may contain colons (e.g. vault/.../ctl).
	const prefix = "raft:"
	rest := key[len(prefix):]
	i := 0
	for i < len(rest) && rest[i] != ':' {
		i++
	}
	if i >= len(rest) {
		return "", false
	}
	return rest[i+1:], true
}
