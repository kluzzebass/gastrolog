package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/convert"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator/pipeline"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
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

// SubmitRetentionRecord routes a single record ejected from a vault during a
// retention event (disposition=route) through the pipeline routing stage with a
// RetentionSource context, so routes matching _source="retention" / _vault=<id>
// fan it out to their configured destinations. It is fire-and-forget (no
// durability ack): partial fan-out is acceptable since the source chunk is
// destroyed regardless. Returns an error only when the pipeline is unavailable
// or the submit is cancelled.
func (o *Orchestrator) SubmitRetentionRecord(ctx context.Context, sourceVaultID glid.GLID, rec chunk.Record, reason string) error {
	o.mu.RLock()
	pl := o.pipeline
	o.mu.RUnlock()
	if pl == nil {
		return errors.New("pipeline not initialized")
	}
	prec := convert.ChunkToRecord(rec)
	return pl.Submit(ctx, routing.Input{
		Record: &prec,
		Source: routing.RetentionSource(sourceVaultID, reason),
	})
}

// SubmitToVault writes a record directly into a named vault's segmentation queue
// on this node, bypassing routing and preserving the record's EventID. It backs
// the direct-to-vault writers (ImportRecords, export-to-vault), which are routed
// to the vault's home node where reconcile keeps a local origin registered.
func (o *Orchestrator) SubmitToVault(ctx context.Context, vaultID glid.GLID, rec chunk.Record, ack chan<- error) error {
	o.mu.RLock()
	pl := o.pipeline
	o.mu.RUnlock()
	if pl == nil {
		return errors.New("pipeline not initialized")
	}
	prec := convert.ChunkToRecord(rec)
	err := pl.SubmitToVault(ctx, vaultID, &prec, ack)
	if errors.Is(err, pipeline.ErrVaultNotRegistered) {
		// The vault's origin is registered on its home node by reconcile; a
		// miss is transient (placement not yet applied here). Surface it as a
		// not-ready condition so callers/clients retry or re-target.
		return ErrVaultNotReady
	}
	return err
}

// pipelineVaultReg records how a vault is currently registered in the pipeline
// supervisor, so reloadPipelineFromConfig re-registers only when something
// material changed: its Home role (placement membership), whether the vault-ctl
// handle is yet available (publisher upgraded from noop to the real
// VaultCtlPublisher), or its chunk rotation policy (the chunking manager
// captures the policy at register, so a policy edit must re-register).
//
// Leadership is deliberately NOT part of this key: the chunking planner reads
// IsLeader() live on every tick, so leadership changes need no re-registration.
type pipelineVaultReg struct {
	home      bool
	hasHandle bool
	policy    chunking.ManifestRotationPolicy
}

// buildPipelineVaultSpec builds the supervisor VaultSpec for a destination vault.
//
// Every node is an Origin for every destination vault (a matched record always
// lands in a local durable segment wherever it is ingested). When the per-vault
// vault-ctl handle is available, the Origin publishes completed-segment metadata
// to the registry via the leader-forwarding applier; otherwise it falls back to
// the noop publisher (single-node/memory mode with no group).
//
// When this node is also a placement member (Home) for the vault with a
// vault-ctl handle, it registers the chunking side (Rubicon D): the leader
// plans the open-chunk manifest via the applier, and every home builds the
// sealed GLCB at seal. LocalHolder is set so segments this node originates
// rename straight to head/ without a self-pull, giving chunking local segments
// to build from.
//
// Peer collection (Rubicon C) is registered additionally only when a segment
// puller is available (cluster mode): roll the registry, pull segments this
// node does not yet hold, and commit holder receipts. Single-node homes have
// no peers to pull from, so they chunk from their own head/ without collection.
func (o *Orchestrator) buildPipelineVaultSpec(vaultID glid.GLID, home bool, fsm *vaultctlfsm.FSM, applier vaultctlfsm.Applier, isLeader func() bool, hasHandle bool, policy chunking.ManifestRotationPolicy) pipeline.VaultSpec {
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
		// Chunking runs on every home with a vault-ctl handle, independent of
		// the peer puller: the leader plans the manifest, all homes build the
		// sealed GLCB. Locate resolves manifest refs under head/ then completed/.
		spec.HomeRoot = root
		spec.FSM = fsm
		spec.Locate = chunking.VaultSegmentLocator{Root: root}
		spec.ChunkRoot = filepath.Join(root, "chunks")
		spec.Applier = applier
		spec.IsLeader = isLeader
		spec.ChunkPolicy = policy
		// Full collection requires a puller to fetch segments originated on
		// other nodes; without peers (single-node) there is nothing to pull.
		if o.segmentPuller != nil {
			spec.Home = true
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
//
// It also returns an isLeader closure reading the vault-CONTROL-PLANE Raft
// leader live (g.Raft.State()==Leader) — distinct from the data-plane
// VaultInstance.IsLeader(). The chunking planner is gated on this so only the
// vault-ctl leader proposes manifest edits; followers apply replicated entries.
func (o *Orchestrator) vaultCtlHandle(vaultID glid.GLID) (*vaultctlfsm.FSM, vaultctlfsm.Applier, func() bool, bool) {
	if o.groupMgr == nil {
		return nil, nil, nil, false
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	g := o.groupMgr.GetGroup(gid)
	if g == nil {
		return nil, nil, nil, false
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	}
	if fsm == nil {
		return nil, nil, nil, false
	}
	var applier vaultctlfsm.Applier
	if o.peerConns != nil {
		applier = cluster.NewVaultCtlChunkApplyForwarder(g.Raft, gid, vaultID, o.peerConns, cluster.ReplicationTimeout)
	} else {
		applier = &vaultCtlApplier{o: o, vaultID: vaultID}
	}
	raft := g.Raft
	isLeader := func() bool { return raft != nil && raft.State() == hraft.Leader }
	return fsm, applier, isLeader, true
}

// resolveChunkPolicy resolves a vault's manifest rotation policy and cron
// expression from the system config. Returns the zero policy and an empty cron
// string when the vault has no rotation policy configured (or the policy is
// missing/invalid). The event-driven thresholds (MaxRecords/MaxBytes/MaxAge)
// are captured by the chunking manager at register; cron is driven separately
// via the shared scheduler.
func (o *Orchestrator) resolveChunkPolicy(sys *system.System, vaultID glid.GLID) (chunking.ManifestRotationPolicy, string) {
	if sys == nil {
		return chunking.ManifestRotationPolicy{}, ""
	}
	vc := findVaultConfig(sys.Config.Vaults, vaultID)
	if vc == nil || vc.RotationPolicyID == nil {
		return chunking.ManifestRotationPolicy{}, ""
	}
	pc := findRotationPolicy(sys.Config.RotationPolicies, *vc.RotationPolicyID)
	if pc == nil {
		return chunking.ManifestRotationPolicy{}, ""
	}
	return manifestRotationPolicy(*pc)
}

// manifestRotationPolicy maps a system rotation policy config onto the pipeline
// chunking manifest policy plus its cron expression. It reuses the same parsing
// as system.RotationPolicyConfig.ToRotationPolicy (the legacy chunk path) so the
// two agree on byte/duration interpretation. Invalid sub-fields are dropped
// rather than failing the whole vault — admission already validated the policy.
func manifestRotationPolicy(c system.RotationPolicyConfig) (chunking.ManifestRotationPolicy, string) {
	var p chunking.ManifestRotationPolicy
	if c.MaxRecords != nil && *c.MaxRecords > 0 {
		p.MaxRecords = uint64(*c.MaxRecords)
	}
	if c.MaxBytes != nil && *c.MaxBytes != "" {
		if b, err := system.ParseBytes(*c.MaxBytes); err == nil {
			p.MaxBytes = b
		}
	}
	if c.MaxAge != nil && *c.MaxAge != "" {
		if d, err := time.ParseDuration(*c.MaxAge); err == nil && d > 0 {
			p.MaxAge = d
		}
	}
	cron := ""
	if c.Cron != nil {
		cron = *c.Cron
	}
	return p, cron
}

// pipelineChunkCronJobName is the scheduler job name for a vault's pipeline
// chunk cron rotation. Distinct from the legacy cronJobName (which rotates a
// chunk.ChunkManager) so the two rotation paths never collide.
func pipelineChunkCronJobName(vaultID glid.GLID) string {
	return "pipeline-chunk-cron:" + vaultID.String()
}

// reconcileChunkCron registers, updates, or removes the per-vault scheduler job
// that drives cron-based manifest rotation for the pipeline. Only the vault-ctl
// leader acts when the job fires (RotateChunkCron no-ops for followers), so the
// job runs on every home and self-selects. Must be called with o.mu held.
func (o *Orchestrator) reconcileChunkCron(vaultID glid.GLID, chunkEnabled bool, cronExpr string) {
	if o.scheduler == nil {
		return
	}
	name := pipelineChunkCronJobName(vaultID)
	if !chunkEnabled || cronExpr == "" {
		o.scheduler.RemoveJob(name)
		return
	}
	if o.scheduler.JobSchedule(name) == cronExpr {
		return
	}
	vid := vaultID
	if err := o.scheduler.UpdateJob(name, cronExpr, func() {
		o.mu.RLock()
		pl := o.pipeline
		o.mu.RUnlock()
		if pl == nil {
			return
		}
		if err := pl.RotateChunkCron(context.Background(), vid); err != nil {
			o.logger.Warn("pipeline chunk cron rotate failed", "vault", vid, "error", err)
		}
	}); err != nil {
		o.logger.Warn("pipeline chunk cron schedule failed", "vault", vid, "cron", cronExpr, "error", err)
		return
	}
	o.scheduler.Describe(name, "Pipeline chunk cron rotation for vault "+vaultID.String())
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

// pipelineVaultChunkRoot returns the segmentation chunk root for a vault when
// this node currently runs the pipeline as a home for it — the directory under
// which pipeline-built sealed GLCBs land (<segmentsDir>/<vaultID>/chunks, the
// "chunks" subdir of originRoot). ok=false when the vault is not a pipeline home
// on this node or no segments base is configured, in which case there are no
// pipeline GLCBs to register. Takes o.mu only to read the registration map and
// segments base; the caller does the stat/registration I/O outside the lock.
// Mirrors the path math in originRoot + buildPipelineVaultSpec (spec.ChunkRoot).
// See gastrolog-2kysn (Rubicon E1).
func (o *Orchestrator) pipelineVaultChunkRoot(vaultID glid.GLID) (string, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	reg, ok := o.pipelineVaults[vaultID]
	if !ok || !reg.home || o.segmentsDir == "" {
		return "", false
	}
	return filepath.Join(o.segmentsDir, vaultID.String(), "chunks"), true
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
// of pipeline-registered vaults. Must be called with o.mu held (it mutates
// o.pipelineVaults).
//
// The desired set is the union of (a) every enabled route's destination vaults
// and (b) every vault homed on this node. (a) registers an Origin on this node
// so a matched record is always durably written to a local segment, regardless
// of where the vault is homed — cross-node collection pulls those segments on
// home nodes. (b) guarantees that any vault this node homes also has a local
// segmentation queue even when no route targets it, which the direct-to-vault
// submit path (ImportRecords, export-to-vault) and retention fan-out require.
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
	// Origin-register every vault homed on this node, even if no route targets
	// it, so direct-to-vault submit always finds a local segmentation queue on
	// the home node it is routed to.
	if sys != nil {
		for i := range sys.Config.Vaults {
			vid := sys.Config.Vaults[i].ID
			if o.isVaultHome(sys, vid) {
				desired[vid] = struct{}{}
			}
		}
	}

	// Unregister vaults that are no longer any route's destination, tearing
	// down their cron rotation job as well.
	for vid := range o.pipelineVaults {
		if _, ok := desired[vid]; !ok {
			o.pipeline.UnregisterVault(vid)
			o.reconcileChunkCron(vid, false, "")
			delete(o.pipelineVaults, vid)
		}
	}

	// Register/re-register each destination vault. Re-register only when its
	// Home role, vault-ctl handle availability, or chunk rotation policy
	// changed, so unchanged vaults never flap their pipeline state. The cron
	// rotation job is reconciled every pass regardless (its schedule may change
	// independent of the registration key, and it is idempotent).
	for vid := range desired {
		home := o.isVaultHome(sys, vid)
		fsm, applier, isLeader, hasHandle := o.vaultCtlHandle(vid)
		policy, cronExpr := o.resolveChunkPolicy(sys, vid)
		chunkEnabled := home && hasHandle
		want := pipelineVaultReg{home: home, hasHandle: hasHandle, policy: policy}
		if prev, ok := o.pipelineVaults[vid]; ok {
			if prev == want {
				o.reconcileChunkCron(vid, chunkEnabled, cronExpr)
				continue
			}
			o.pipeline.UnregisterVault(vid)
			delete(o.pipelineVaults, vid)
		}
		if err := o.pipeline.RegisterVault(o.buildPipelineVaultSpec(vid, home, fsm, applier, isLeader, hasHandle, policy)); err != nil {
			return fmt.Errorf("register vault %s: %w", vid, err)
		}
		o.pipelineVaults[vid] = want
		o.reconcileChunkCron(vid, chunkEnabled, cronExpr)
	}

	return nil
}
