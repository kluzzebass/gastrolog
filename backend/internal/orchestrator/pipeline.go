package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator/pipeline"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ServeSegmentPull streams a locally-held completed segment to w for a remote
// collector. Wired to the cluster PullSegment RPC handler so home nodes can
// fetch segments this node originated or already holds (Rubicon C).
func (o *Orchestrator) ServeSegmentPull(vaultID, segmentID glid.GLID, w io.Writer) error {
	o.mu.RLock()
	pl := o.pipeline
	o.mu.RUnlock()
	if pl == nil {
		return errors.New("pipeline not initialized")
	}
	return pl.ServePull(distribution.PullRequest{VaultID: vaultID, SegmentID: segmentID, Dest: w})
}

// pipelineVaultReg records how a vault is currently registered in the pipeline
// supervisor, so reloadPipelineFromConfig re-registers only when something
// material changed: its Home role (placement membership) or whether the
// vault-ctl handle is yet available (publisher upgraded from noop to the real
// VaultCtlPublisher).
type pipelineVaultReg struct {
	home      bool
	hasHandle bool
}

// buildPipelineVaultSpec builds the supervisor VaultSpec for a destination vault.
//
// Every node is an Origin for every destination vault (a matched record always
// lands in a local durable segment wherever it is ingested). When the per-vault
// vault-ctl handle is available, the Origin publishes completed-segment metadata
// to the registry via the leader-forwarding applier; otherwise it falls back to
// the noop publisher (single-node/memory mode with no group).
//
// When this node is also a placement member (Home) for the vault, it registers
// the collection side (Rubicon C): roll the registry, pull segments it does not
// yet hold, and commit holder receipts. LocalHolder is set so segments this node
// originates rename straight to head/ without a self-pull.
func (o *Orchestrator) buildPipelineVaultSpec(vaultID glid.GLID, home bool, fsm *vaultctlfsm.FSM, applier vaultctlfsm.Applier, hasHandle bool) pipeline.VaultSpec {
	root := o.originRoot(vaultID)
	spec := pipeline.VaultSpec{
		VaultID:    vaultID,
		Origin:     true,
		OriginRoot: root,
		Publisher:  noopPublisher{},
	}
	if hasHandle {
		spec.Publisher = &distribution.VaultCtlPublisher{
			Applier:      applier,
			OriginNodeID: o.localNodeID,
		}
	}
	if home && hasHandle {
		// This node holds everything it originates locally (completed→head).
		spec.LocalHolder = func() bool { return true }
		// Full collection requires a puller to fetch segments originated on
		// other nodes; without peers (single-node) there is nothing to pull.
		if o.segmentPuller != nil {
			spec.Home = true
			spec.HomeRoot = root
			spec.FSM = fsm
			spec.Log = &segmentLogReader{fsm: fsm, localNodeID: o.localNodeID}
			spec.Pull = &segmentPullClient{fsm: fsm, puller: o.segmentPuller, localNodeID: o.localNodeID}
			spec.Receipts = &segmentReceiptCommitter{applier: applier, localNodeID: o.localNodeID}
		}
	}
	return spec
}

// vaultCtlHandle resolves the per-vault vault-ctl FSM and an applier for it
// (leader-forwarding in cluster mode, local in single-node mode). Returns
// ok=false when the vault-ctl group is not present on this node yet — every
// cluster node is a voter in every vault-ctl group, so this is normally only
// transient during startup or single-node/memory mode without a group manager.
func (o *Orchestrator) vaultCtlHandle(vaultID glid.GLID) (*vaultctlfsm.FSM, vaultctlfsm.Applier, bool) {
	if o.groupMgr == nil {
		return nil, nil, false
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	g := o.groupMgr.GetGroup(gid)
	if g == nil {
		return nil, nil, false
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	}
	if fsm == nil {
		return nil, nil, false
	}
	var applier vaultctlfsm.Applier
	if o.peerConns != nil {
		applier = cluster.NewVaultCtlChunkApplyForwarder(g.Raft, gid, vaultID, o.peerConns, cluster.ReplicationTimeout)
	} else {
		applier = &vaultCtlApplier{o: o, vaultID: vaultID}
	}
	return fsm, applier, true
}

// isVaultHome reports whether the local node is a placement member (leader or
// follower) for the vault — i.e. a desired holder under the implicit holder
// model.
func (o *Orchestrator) isVaultHome(sys *system.System, vaultID glid.GLID) bool {
	for i := range sys.Config.Vaults {
		v := &sys.Config.Vaults[i]
		if v.ID != vaultID {
			continue
		}
		nscs := sys.Runtime.NodeStorageConfigs
		if system.LeaderNodeID(v.Placements, nscs) == o.localNodeID {
			return true
		}
		return slices.Contains(system.FollowerNodeIDs(v.Placements, nscs), o.localNodeID)
	}
	return false
}

// originRoot returns the segmentation root for a vault: the directory that
// holds the vault's working/ and completed/ segment areas on this node.
//
// When no segments base is configured (no node home / SegmentsDir, e.g. in
// embedded tests) it lazily allocates one isolated temp base — shared across
// the orchestrator's vaults — so origin registration never writes segments
// into the process working directory. Production always sets SegmentsDir.
// Callers hold o.mu (originRoot is only reached via reloadPipelineFromConfig),
// so the one-time assignment to o.segmentsDir is safe.
//
// TODO(gastrolog-jiwlf): segment storage location is currently a single base
// dir under the node home. Make it configurable per node/storage class.
func (o *Orchestrator) originRoot(vaultID glid.GLID) string {
	if o.segmentsDir == "" {
		tmp, err := os.MkdirTemp("", "gastrolog-segments-")
		if err != nil {
			tmp = filepath.Join(os.TempDir(), "gastrolog-segments")
		}
		o.segmentsDir = tmp
	}
	return filepath.Join(o.segmentsDir, vaultID.String())
}

// noopPublisher is the distribution publisher used while no vault-ctl handle is
// available (single-node/memory mode): it accepts and drops publish metadata.
// With a handle, the VaultCtlPublisher takes over so completed segments are
// announced to the vault-ctl log and pulled by home nodes.
type noopPublisher struct{}

var _ distribution.Publisher = noopPublisher{}

func (noopPublisher) Publish(context.Context, distribution.Metadata) error { return nil }

// buildRoutingTable compiles a routing table from the cluster-wide route
// config. Routing is vault-only: each enabled route maps its match expression
// to its destination vault IDs, independent of node placement — every node is
// an Origin for every destination vault, so records are written to a local
// durable segment wherever they are ingested.
func buildRoutingTable(sys *system.System) (*routing.Table, error) {
	if sys == nil {
		return routing.NewTable(nil), nil
	}
	var routes []*routing.Route
	for _, rc := range sys.Config.Routes {
		if !rc.Enabled {
			continue
		}
		r, err := routing.CompileRoute(rc.ID, rc.Name, rc.Priority, rc.MatchExpression(), rc.Destinations)
		if err != nil {
			return nil, fmt.Errorf("compile route %s: %w", rc.Name, err)
		}
		routes = append(routes, r)
	}
	return routing.NewTable(routes), nil
}

// destinationVaults returns the set of vault IDs that any enabled route targets
// — the vaults this node must Origin-register so matched records have a local
// segmentation queue to land in.
func destinationVaults(sys *system.System) map[glid.GLID]struct{} {
	dests := make(map[glid.GLID]struct{})
	if sys == nil {
		return dests
	}
	for _, rc := range sys.Config.Routes {
		if !rc.Enabled {
			continue
		}
		for _, vid := range rc.Destinations {
			dests[vid] = struct{}{}
		}
	}
	return dests
}

// reloadPipelineFromConfig republishes the routing table and reconciles the set
// of pipeline-registered vaults to match the destinations of all enabled
// routes. Must be called with o.mu held (it mutates o.pipelineVaults).
//
// Every route-target vault is registered as an Origin on this node so a matched
// record is always durably written to a local segment, regardless of where the
// vault is homed — cross-node collection pulls those segments on home nodes.
func (o *Orchestrator) reloadPipelineFromConfig(sys *system.System) error {
	if o.pipeline == nil {
		return nil
	}

	table, err := buildRoutingTable(sys)
	if err != nil {
		return err
	}
	o.pipeline.SetRoutingTable(table)

	desired := destinationVaults(sys)

	// Unregister vaults that are no longer any route's destination.
	for vid := range o.pipelineVaults {
		if _, ok := desired[vid]; !ok {
			o.pipeline.UnregisterVault(vid)
			delete(o.pipelineVaults, vid)
		}
	}

	// Register/re-register each destination vault. Re-register only when its
	// Home role or vault-ctl handle availability changed, so unchanged vaults
	// never flap their pipeline state.
	for vid := range desired {
		home := o.isVaultHome(sys, vid)
		fsm, applier, hasHandle := o.vaultCtlHandle(vid)
		want := pipelineVaultReg{home: home, hasHandle: hasHandle}
		if prev, ok := o.pipelineVaults[vid]; ok {
			if prev == want {
				continue
			}
			o.pipeline.UnregisterVault(vid)
			delete(o.pipelineVaults, vid)
		}
		if err := o.pipeline.RegisterVault(o.buildPipelineVaultSpec(vid, home, fsm, applier, hasHandle)); err != nil {
			return fmt.Errorf("register vault %s: %w", vid, err)
		}
		o.pipelineVaults[vid] = want
	}

	return nil
}
