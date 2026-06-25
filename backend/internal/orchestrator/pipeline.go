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
	// sealed GLCB at seal. LocalHolder marks this node as a home holder so
	// segments promote to head/ only after vault-ctl publishes the registry
	// entry, giving chunking local segments to build from.
//
// Peer collection (Rubicon C) is registered additionally only when a segment
// puller is available (cluster mode): roll the registry, pull segments this
// node does not yet hold, and commit holder receipts. Single-node homes have
// no peers to pull from, so they chunk from their own head/ without collection.
func (o *Orchestrator) buildPipelineVaultSpec(vaultID glid.GLID, home bool, fsm *vaultctlfsm.FSM, applier vaultctlfsm.Applier, isLeader func() bool, hasHandle bool, policy chunking.ManifestRotationPolicy) (pipeline.VaultSpec, error) {
	root, err := o.originRoot(vaultID)
	if err != nil {
		return pipeline.VaultSpec{}, err
	}
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
		spec.LookupFSM = func() *vaultctlfsm.FSM {
			f, _, _, ok := o.vaultCtlHandle(vaultID)
			if ok && f != nil {
				return f
			}
			return fsm
		}
		spec.Locate = chunking.VaultSegmentLocator{Root: root}
		spec.ChunkRoot = filepath.Join(root, "chunks")
		spec.Applier = applier
		spec.IsLeader = isLeader
		spec.ChunkPolicy = policy
		spec.OnChunkBuilt = func(id chunk.ChunkID) {
			o.registerBuiltPipelineChunk(vaultID, fsm, id)
		}
		spec.ChunkRequiredHolders = func() []string {
			return o.vaultPlacementNodeIDs(vaultID)
		}
		// Full collection requires a puller to fetch segments originated on
		// other nodes; without peers (single-node) there is nothing to pull.
		if o.segmentPuller != nil {
			spec.Home = true
			lookup := spec.LookupFSM
			spec.Log = &segmentLogReader{lookup: lookup, localNodeID: o.localNodeID, vaultRoot: root}
			spec.Pull = &segmentPullClient{
				lookup:      lookup,
				puller:      o.segmentPuller,
				localNodeID: o.localNodeID,
				vaultRoot:   root,
			}
			spec.Receipts = &segmentReceiptCommitter{applier: applier, localNodeID: o.localNodeID}
		}
	}
	return spec, nil
}

// registerBuiltPipelineChunk registers a freshly-built pipeline GLCB with the
// local chunk manager when the FSM already shows the chunk Sealed. This
// closes the build-finishes-last ordering gap: the reconciler's onSeal
// callback registers the GLCB only when the file exists on disk, so a home
// whose build completes AFTER CmdSealChunk applied would otherwise never
// register it — its local queries would silently miss the chunk. When the
// chunk is not Sealed yet (build-finishes-first ordering), this is a no-op
// and the later onSeal registers it.
func (o *Orchestrator) registerBuiltPipelineChunk(vaultID glid.GLID, fsm *vaultctlfsm.FSM, id chunk.ChunkID) {
	e := fsm.Get(id)
	if e == nil {
		return
	}
	// Register as soon as the GLCB exists so sealing chunks are queryable
	// before CmdSealChunk; fully sealed chunks still register here when build
	// finishes after seal (registerBuiltPipelineChunk ordering gap).
	if e.State != chunk.ChunkStateSealed && e.State != chunk.ChunkStateSealing {
		return
	}
	ti := o.findLocalVaultInstance(vaultID)
	if ti == nil || ti.Reconciler == nil {
		return
	}
	ti.Reconciler.registerPipelineGLCB(*e)
}

// vaultPlacementNodeIDs returns the node IDs of every placement member for a
// vault, used to gate ReleaseSegments until each home has holder receipts.
func (o *Orchestrator) vaultPlacementNodeIDs(vaultID glid.GLID) []string {
	if o.sysLoader == nil {
		return nil
	}
	sys, err := o.sysLoader.Load(context.Background())
	if err != nil || sys == nil {
		return nil
	}
	for i := range sys.Config.Vaults {
		v := &sys.Config.Vaults[i]
		if v.ID == vaultID {
			return system.PlacementNodeIDs(v.Placements, sys.Runtime.NodeStorageConfigs)
		}
	}
	return nil
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
	localID := o.localNodeID
	isLeader := func() bool {
		if raft == nil || raft.State() != hraft.Leader {
			return false
		}
		_, id := raft.LeaderWithID()
		return id != "" && string(id) == localID
	}
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

// originRoot returns the segmentation root for a vault under the configured
// node home (SegmentsDir/<vaultID>). Callers must set Config.SegmentsDir from
// home.Dir.SegmentsDir() before enabling the pipeline.
func (o *Orchestrator) originRoot(vaultID glid.GLID) (string, error) {
	if o.segmentsDir == "" {
		return "", errors.New("segments directory unset: configure orchestrator.Config.SegmentsDir from node home")
	}
	return filepath.Join(o.segmentsDir, vaultID.String()), nil
}

// isPipelineIngestVault reports whether this vault receives records through the
// segmentation pipeline (routing → segments → chunking → GLCB at ChunkRoot).
//
// Pipeline ingest vaults do not use the legacy chunk-manager path: no append to
// m.active, no PostSealProcess/sealToGLCB in storage/disk-*, and no record-stream
// replication or missing-replica catchup. Sealed bytes are produced once by the
// pipeline; query access is registerPipelineGLCB (external path registration).
func (o *Orchestrator) isPipelineIngestVault(vaultID glid.GLID) bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	_, ok := o.pipelineVaults[vaultID]
	o.mu.RUnlock()
	return ok
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
	segmentsDir := o.segmentsDir
	reg, registered := o.pipelineVaults[vaultID]
	o.mu.Unlock()
	if segmentsDir == "" {
		return "", false
	}
	root := filepath.Join(segmentsDir, vaultID.String(), "chunks")
	if registered && reg.home {
		return root, true
	}
	return "", false
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

	// Align each vault's vault-ctl Raft leadership with its placement leader.
	// Every cluster node is a voter in every vault-ctl group, so an election
	// can otherwise land leadership on a non-home node — and the chunking
	// planner (home ∧ vault-ctl leader) would then run nowhere, stalling
	// manifest planning for the vault cluster-wide. The leader epoch's
	// reconcile pass performs the actual LeadershipTransferToServer.
	if sys != nil && o.vaultCtlLeaders != nil {
		for i := range sys.Config.Vaults {
			v := &sys.Config.Vaults[i]
			o.vaultCtlLeaders.SetDesiredLeaderID(v.ID,
				system.LeaderNodeID(v.Placements, sys.Runtime.NodeStorageConfigs))
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
				o.finishPendingPipelineCtlRestore(vid)
				continue
			}
			o.pipeline.UnregisterVault(vid)
			delete(o.pipelineVaults, vid)
		}
		spec, err := o.buildPipelineVaultSpec(vid, home, fsm, applier, isLeader, hasHandle, policy)
		if err != nil {
			return fmt.Errorf("build pipeline vault %s: %w", vid, err)
		}
		if err := o.pipeline.RegisterVault(spec); err != nil {
			return fmt.Errorf("register vault %s: %w", vid, err)
		}
		o.pipelineVaults[vid] = want
		o.reconcileChunkCron(vid, chunkEnabled, cronExpr)
		o.finishPendingPipelineCtlRestore(vid)
	}

	for vid := range desired {
		if err := o.rewirePipelineAfterCtlRestore(vid); err != nil {
			o.logger.Warn("pipeline rewire after config reload failed",
				"vault", vid, "error", err)
		}
	}

	return nil
}

// overlayPipelineChunkMetaBounds fills missing timestamp bounds on active/sealing
// pipeline chunks from a built GLCB or, when absent, from the replicated manifest
// (and local segment refs when manifest bounds are not yet populated).
func (o *Orchestrator) overlayPipelineChunkMetaBounds(vaultID glid.GLID, m *chunk.ChunkMeta) {
	if m == nil || chunkMetaBoundsComplete(m) {
		return
	}
	if m.State != chunk.ChunkStateActive && m.State != chunk.ChunkStateSealing {
		return
	}
	if o.applyCachedPipelineChunkBounds(m) {
		return
	}
	if o.overlayPipelineChunkMetaBoundsFromGLCB(vaultID, m) {
		o.storeCachedPipelineChunkBounds(m)
		return
	}
	o.overlayPipelineChunkMetaBoundsFromManifest(vaultID, m)
	o.storeCachedPipelineChunkBounds(m)
}

type pipelineChunkBoundsOverlay struct {
	WriteEnd    time.Time
	IngestStart time.Time
	IngestEnd   time.Time
	SourceEnd   time.Time
}

func (o *Orchestrator) applyCachedPipelineChunkBounds(m *chunk.ChunkMeta) bool {
	v, ok := o.pipelineChunkBoundsCache.Load(m.ID)
	if !ok {
		return false
	}
	b, ok := v.(pipelineChunkBoundsOverlay)
	if !ok {
		return false
	}
	if !b.WriteEnd.IsZero() {
		m.WriteEnd = b.WriteEnd
	}
	if !b.IngestStart.IsZero() {
		m.IngestStart = b.IngestStart
	}
	if !b.IngestEnd.IsZero() {
		m.IngestEnd = b.IngestEnd
	}
	if !b.SourceEnd.IsZero() {
		m.SourceEnd = b.SourceEnd
	}
	return chunkMetaBoundsComplete(m)
}

func (o *Orchestrator) storeCachedPipelineChunkBounds(m *chunk.ChunkMeta) {
	if m == nil || !chunkMetaBoundsComplete(m) {
		return
	}
	o.pipelineChunkBoundsCache.Store(m.ID, pipelineChunkBoundsOverlay{
		WriteEnd:    m.WriteEnd,
		IngestStart: m.IngestStart,
		IngestEnd:   m.IngestEnd,
		SourceEnd:   m.SourceEnd,
	})
}

func (o *Orchestrator) overlayPipelineChunkMetaBoundsFromGLCB(vaultID glid.GLID, m *chunk.ChunkMeta) bool {
	chunkRoot, ok := o.pipelineVaultChunkRoot(vaultID)
	if !ok {
		return false
	}
	glcbPath := chunking.ChunkGLCBPath(chunkRoot, m.ID)
	if _, err := os.Stat(glcbPath); err != nil {
		return false
	}
	result, err := chunking.BuildResultFromExistingGLCB(glcbPath, time.Time{})
	if err != nil {
		return false
	}
	if !result.WriteEnd.IsZero() {
		m.WriteEnd = result.WriteEnd
	}
	if !result.IngestStart.IsZero() {
		m.IngestStart = result.IngestStart
	}
	if !result.IngestEnd.IsZero() {
		m.IngestEnd = result.IngestEnd
	}
	if !result.SourceEnd.IsZero() {
		m.SourceEnd = result.SourceEnd
	}
	return true
}

func (o *Orchestrator) overlayPipelineChunkMetaBoundsFromManifest(vaultID glid.GLID, m *chunk.ChunkMeta) {
	manifest := o.pipelineChunkManifest(vaultID, m.ID)
	if manifest == nil {
		return
	}
	if !manifest.Bounds.IsZero() {
		vaultctlfsm.ApplyManifestBoundsToChunkMeta(m, manifest.Bounds)
		return
	}
	root, err := o.originRoot(vaultID)
	if err != nil {
		return
	}
	bounds, err := chunking.BoundsFromManifestRefs(manifest.Refs, chunking.VaultSegmentLocator{Root: root})
	if err != nil {
		return
	}
	vaultctlfsm.ApplyManifestBoundsToChunkMeta(m, bounds)
}

func (o *Orchestrator) pipelineChunkManifest(vaultID glid.GLID, chunkID chunk.ChunkID) *vaultctlfsm.OpenChunkManifest {
	if o.groupMgr == nil {
		return nil
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return nil
	}
	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil
	}
	sub := vfsm.VaultFSM(vaultID)
	if sub == nil {
		return nil
	}
	if sm := sub.SealedManifest(); sm != nil && sm.ChunkID == chunkID {
		return sm
	}
	if oc := sub.OpenChunk(); oc != nil && oc.ChunkID == chunkID {
		return oc
	}
	return nil
}

func chunkMetaBoundsComplete(m *chunk.ChunkMeta) bool {
	if m == nil {
		return true
	}
	return saneRecordTime(m.WriteStart) && saneRecordTime(m.WriteEnd) &&
		saneRecordTime(m.IngestStart) && saneRecordTime(m.IngestEnd)
}

func saneRecordTime(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 2000
}
