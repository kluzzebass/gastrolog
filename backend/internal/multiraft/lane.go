package multiraft

import (
	"strings"
)

// RaftLaneSNIPrefix is the TLS SNI prefix for per-group inbound raft lanes.
// Each group uses SNIPrefix + "." + encodeGroupID(groupID), e.g.
// gastrolog-raft.config or gastrolog-raft.vault.<id>.ctl.
const RaftLaneSNIPrefix = "gastrolog-raft"

// LegacyRaftLaneSNI is the pre-per-group raft lane name. Inbound connections
// with this SNI are routed to the cluster config group ("config").
const LegacyRaftLaneSNI = RaftLaneSNIPrefix

// ClusterConfigGroupID is the multiraft group ID for cluster-ctl Raft RPCs.
const ClusterConfigGroupID = "config"

// LaneSNI returns the TLS ServerName for outbound dials and inbound demux to
// an isolated raft lane for groupID.
func LaneSNI(groupID string) string {
	if groupID == "" {
		return LegacyRaftLaneSNI
	}
	return RaftLaneSNIPrefix + "." + encodeGroupIDForSNI(groupID)
}

// GroupIDFromLaneSNI maps an inbound TLS SNI to a multiraft group ID.
func GroupIDFromLaneSNI(sni string) (string, bool) {
	if sni == LegacyRaftLaneSNI {
		return ClusterConfigGroupID, true
	}
	const dot = RaftLaneSNIPrefix + "."
	if !strings.HasPrefix(sni, dot) {
		return "", false
	}
	return decodeGroupIDFromSNI(strings.TrimPrefix(sni, dot)), true
}

// IsRaftLaneSNI reports whether sni selects any raft lane (legacy or per-group).
func IsRaftLaneSNI(sni string) bool {
	_, ok := GroupIDFromLaneSNI(sni)
	return ok
}

func encodeGroupIDForSNI(groupID string) string {
	return strings.ReplaceAll(groupID, "/", ".")
}

func decodeGroupIDFromSNI(encoded string) string {
	return strings.ReplaceAll(encoded, ".", "/")
}
