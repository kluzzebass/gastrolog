package orchestrator

import (
	"gastrolog/internal/cluster"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftlevel"
)

// clusterWiring is the set of cluster collaborators ApplyConfig installs from
// its factories. It is immutable once published; ApplyConfig publishes a new
// one whole.
type clusterWiring struct {
	// groupMgr is the shared multi-group Raft manager (system, vault ctl, …),
	// used to create and tear down vault-ctl groups.
	groupMgr *raftgroup.GroupManager

	// peerConns is the shared gRPC pool for cluster peers, used by
	// ApplyVaultControlPlane forwarding.
	peerConns *cluster.PeerConnManager

	// vaultCtlGate gates every vault-ctl proposal from this node.
	vaultCtlGate *raftlevel.Gate

	// vaultCtlReplicated is true when vault metadata replicates through
	// per-vault vault-ctl Raft groups on this node (a GroupManager and a node
	// address resolver are both wired). A local instance whose group does not
	// exist yet then reports not-ready; without it there is no group to wait
	// for and the instance's own chunk manager is authoritative.
	vaultCtlReplicated bool

	// segmentPuller streams completed segments from peer holders for
	// collection (cluster mode only).
	segmentPuller *cluster.SegmentPuller

	// chunkGLCBPuller streams sealed chunk GLCBs from peer homes for
	// replica catch-up — a home that missed builds (wedged, down) whose
	// source segments were already released has no other recovery path.
	// Concurrent sweeps never pull the same GLCB twice, and the in-flight
	// budget is never overshot, because both are claimed on the scheduler's
	// job map (RunOnceIfAbsentUnderLimit in pullMissingGLCB) rather than in a
	// second inflight map of the orchestrator's own.
	chunkGLCBPuller *cluster.ChunkGLCBPuller
}

var noWiring = &clusterWiring{}

// wiring returns the published cluster wiring; before ApplyConfig, an empty
// one.
func (o *Orchestrator) wiring() *clusterWiring {
	if w := o.wired.Load(); w != nil {
		return w
	}
	return noWiring
}

// groupManager returns the vault-ctl group manager, or nil before ApplyConfig
// wires one.
func (o *Orchestrator) groupManager() *raftgroup.GroupManager {
	return o.wiring().groupMgr
}
