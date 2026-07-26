package orchestrator_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/vaultraft"

	hraft "github.com/hashicorp/raft"
)

// vaultTypeKey is the string form of the file-instance type used as a
// factory-map key. File instance is used (rather than memory instance) because
// only the file-instance chunk Manager implements SetAnnouncer — the pathway
// that propagates chunk metadata events through vault-ctl Raft to
// followers. Without announcements, replication tests are vacuous.
const vaultTypeKey = string(system.VaultTypeFile)

// harnessStorageClass is a non-zero storage class so findLocalFileStorage
// matches the NodeStorageConfig we seed. Value is arbitrary; zero is
// reserved to mean "no local file storage."
const harnessStorageClass uint32 = 1

// orchRelNode bundles every piece of per-node state for the orchestrator-
// backed reliability harness: cluster gRPC server for multiraft transport,
// a raftwal to back vault-ctl Raft groups, a GroupManager, a real
// orchestrator, and bookkeeping for restart.
//
// id is the node's LocalNodeID string — the same value the orchestrator
// reports as its own identity and the same value the Raft resolver keys
// on. buildVaultRaftMembers converts NodeConfig.ID (a GLID) to its string
// form and calls NodeAddressResolver with that, so id here MUST be the
// GLID string form, not a human-readable label.
type orchRelNode struct {
	id            string // GLID string; also orchestrator LocalNodeID
	label         string // human label for test output ("node-1" etc.)
	home          string
	fileStorageID glid.GLID // FileStorage.ID for this node's chunk directory
	clusterSrv    *cluster.Server
	wal           *raftwal.WAL
	groupMgr      *raftgroup.GroupManager
	orch          *orchestrator.Orchestrator
	peerConns     *cluster.PeerConnManager // non-nil only with withPipelineCluster
	// factories is the node's component-factory set as built by startNode,
	// kept so tests can drive dispatcher-equivalent calls (AddVaultInstance)
	// after config changes.
	factories orchestrator.Factories
	cancel    context.CancelFunc
}

// orchRelHarness boots N in-process nodes, each running a real orchestrator
// with a vault-ctl Raft group replicated across all nodes. Unlike the
// lower-level reliability harness in backend/internal/vaultraft, this one
// exercises the full orchestrator wiring: ApplyConfig, AppendToVault,
// ListAllChunkMetas, the scheduler, vault readiness gating, and the
// vault-ctl Raft group built via createVaultRaftGroupVaultCtl.
//
// Cross-node cluster RPCs (RecordForwarder, RemoteTransferrer,
// ChunkReplicator) are left nil — scenarios that need them should stub via
// direct in-process shims. The default harness exercises replication
// through vault-ctl Raft only, which is the primary target for
// metadata-divergence tests.
type orchRelHarness struct {
	t        *testing.T
	nodes    map[string]*orchRelNode
	nodeIDs  []string
	cfgStore system.Store
	// vaultID is the default (first) vault's identifier; kept as a top-level
	// field for the single-vault convenience API.
	vaultID glid.GLID
	// vaults holds every configured vault, with the default vault as
	// vaults[0]. Multi-vault scenarios use addVaultSpec during setup to
	// add more, each with its own node subset.
	vaults       []vaultSpec
	sharedCtx    context.Context
	sharedCancel context.CancelFunc

	// pipeline, when non-nil, enables the full cross-node pipeline wiring
	// (Rubicon E3): a static-resolver PeerConns pool per node (segment pulls
	// + vault-ctl apply forwarding), the cluster PullSegment server, a tight
	// segment complete policy, and a record-count chunk rotation policy on every
	// vault so ingest converges to sealed GLCBs quickly in tests.
	pipeline *pipelineClusterOpts
	// routeVaultIdxs lists vaults (indexes into h.vaults) that get an
	// enabled match-all route seeded in the shared config.
	routeVaultIdxs []int
	// rotationPolicyID is the shared pipeline rotation policy written by
	// seedSharedConfig (nil without withPipelineCluster); vaults added
	// mid-test (addRuntimeVault) reference it too.
	rotationPolicyID *glid.GLID
}

// pipelineClusterOpts carries the pipeline tuning for withPipelineCluster.
type pipelineClusterOpts struct {
	completePolicy  segmentation.CompletePolicy
	chunkMaxRecords int64
}

// vaultSpec identifies one vault in the harness along with which nodes
// participate in its vault-ctl Raft group. First node in nodeIdxs is the
// placement leader. For multi-vault scenarios, use orchRelOptions to
// register additional vaultSpecs before startup.
type vaultSpec struct {
	label    string    // human label for test output ("A", "B", ...)
	id       glid.GLID // vault GLID
	nodeIdxs []int     // indexes into h.nodeIDs; first is vault leader
}

// orchRelOption configures a harness before it boots. Applied between
// nodeID assignment and cfgStore seeding, so options can influence what
// gets written to the config store.
type orchRelOption func(*orchRelHarness)

// withExtraVault registers an additional vault placed on the given
// node indexes (into h.nodeIDs). The first index is the vault leader.
// len(nodeIdxs) must be an odd number >= 1 for valid Raft quorum, and
// each index must be a valid h.nodeIDs index. The vault is labeled
// "B" (or "C", "D", ...) based on insertion order.
func withExtraVault(nodeIdxs []int) orchRelOption {
	return func(h *orchRelHarness) {
		label := string(rune('B' + len(h.vaults) - 1))
		// Vault and instance share the same ID — instance ID equals vault ID.
		id := glid.New()
		h.vaults = append(h.vaults, vaultSpec{
			label:    label,
			id:       id,
			nodeIdxs: nodeIdxs,
		})
	}
}

// withPipelineCluster wires the real cross-node pipeline transport on every
// node and tunes segment close / chunk rotation for fast test convergence.
// completePolicy controls when working segments complete; chunkMaxRecords seals
// the open-chunk manifest once it references that many records.
func withPipelineCluster(completePolicy segmentation.CompletePolicy, chunkMaxRecords int64) orchRelOption {
	return func(h *orchRelHarness) {
		h.pipeline = &pipelineClusterOpts{
			completePolicy:  completePolicy,
			chunkMaxRecords: chunkMaxRecords,
		}
	}
}

// withMatchAllRoute seeds an enabled match-all ("*") route targeting the
// vault at the given index into h.vaults (0 = default vault). Records
// submitted through the pipeline routing path fan out to that vault.
func withMatchAllRoute(vaultIdx int) orchRelOption {
	return func(h *orchRelHarness) {
		h.routeVaultIdxs = append(h.routeVaultIdxs, vaultIdx)
	}
}

const (
	// Harness convergence waits are progress-based (see waitProgress), not
	// wall-clock-bounded: the awaited work is CPU-bound in-process cluster
	// activity, so under multi-suite contention a fixed wall-clock budget buys
	// an arbitrary fraction of the compute (gastrolog-5h1uq2). A wait fails
	// only when its observed progress metric has not changed for this stall
	// window. The window sits well above the slowest legitimate quiet period
	// between progress events — the 20s vault catch-up sweep cron — with
	// headroom for contention stretching a tick.
	orchHarnessStallWindow = 60 * time.Second
	// orchHarnessHardBackstop bounds total wait time even while progress
	// trickles, so a livelocked metric (one that keeps changing without ever
	// converging — election churn, oscillating counts) still fails with
	// diagnostics instead of hanging the package. Steady progress toward the
	// goal finishes far inside this; only a true wedge or livelock reaches it.
	orchHarnessHardBackstop = 5 * time.Minute
)

// orchRelHarnessSlots caps how many orchRel harnesses run concurrently in
// this package. Each harness boots 3-4 full in-process nodes (vault-ctl Raft
// groups, schedulers, pipelines); the reliability family already dominates
// package runtime, and unbounded t.Parallel() fan-out multiplies CPU demand
// until the family starves itself under full-suite load. Progress-based
// waits tolerate slowness, but bounding intra-package parallelism keeps
// convergence moving. Tests keep t.Parallel() (they still overlap two at a
// time and with non-harness tests) and keep running in `go test ./...`.
var orchRelHarnessSlots = make(chan struct{}, 2)

// progressPoint is one observed change of a wait's progress metric, kept for
// the trajectory report on failure.
type progressPoint struct {
	elapsed time.Duration
	state   string
}

// waitProgress is the harness's convergence-wait primitive. It polls sample
// every interval until sample reports done. The wait fails on STALL — the
// progress string unchanged for orchHarnessStallWindow — never on total
// elapsed time alone, so steady progress is never killed mid-convergence
// under CPU contention. A generous hard backstop (orchHarnessHardBackstop)
// still bounds the total, catching livelocks where the metric keeps changing
// without converging.
//
// sample returns (progress, done): done=true ends the wait successfully;
// progress is an opaque human-readable snapshot of the awaited metric — ANY
// change resets the stall clock and is appended to the trajectory reported
// on failure. onFail (optional) runs scenario diagnostics (dumpPipelineState,
// goroutine profiles, ...) before the Fatalf.
func (h *orchRelHarness) waitProgress(what string, interval time.Duration, sample func() (string, bool), onFail func()) {
	h.t.Helper()
	start := time.Now()
	progress, done := sample()
	if done {
		return
	}
	trajectory := []progressPoint{{0, progress}}
	lastChange := start
	for {
		time.Sleep(interval)
		next, done := sample()
		now := time.Now()
		if done {
			// Slow-but-successful waits log their trajectory so a wait that
			// nearly stalled (legitimate quiet period approaching the window)
			// is visible without failing.
			if now.Sub(start) >= orchHarnessStallWindow/2 {
				h.t.Logf("%s: converged after %s (%d progress changes)\n%s",
					what, now.Sub(start).Round(time.Millisecond), len(trajectory), formatTrajectory(trajectory))
			}
			return
		}
		if next != progress {
			progress = next
			lastChange = now
			trajectory = append(trajectory, progressPoint{now.Sub(start), next})
		}
		stalledFor := now.Sub(lastChange)
		stalled := stalledFor >= orchHarnessStallWindow
		backstopped := now.Sub(start) >= orchHarnessHardBackstop
		if !stalled && !backstopped {
			continue
		}
		if onFail != nil {
			onFail()
		}
		reason := "progress stalled"
		if backstopped && !stalled {
			reason = "hit hard backstop while still changing (livelock?)"
		}
		h.t.Fatalf("%s: %s after %s (no progress for %s; stall window %s, hard backstop %s)\nprogress trajectory (%d changes):\n%s",
			what, reason, now.Sub(start).Round(time.Millisecond), stalledFor.Round(time.Millisecond),
			orchHarnessStallWindow, orchHarnessHardBackstop, len(trajectory), formatTrajectory(trajectory))
	}
}

// formatTrajectory renders the progress trajectory for a failed wait. Long
// trajectories are elided in the middle — the head shows the starting state,
// the tail shows where progress stopped.
func formatTrajectory(points []progressPoint) string {
	const headKeep, tailKeep = 8, 24
	var b []byte
	appendPoint := func(p progressPoint) {
		b = append(b, []byte(fmt.Sprintf("  +%-10s %s\n", p.elapsed.Round(time.Millisecond), p.state))...)
	}
	if len(points) <= headKeep+tailKeep {
		for _, p := range points {
			appendPoint(p)
		}
		return string(b)
	}
	for _, p := range points[:headKeep] {
		appendPoint(p)
	}
	b = append(b, []byte(fmt.Sprintf("  ... %d changes elided ...\n", len(points)-headKeep-tailKeep))...)
	for _, p := range points[len(points)-tailKeep:] {
		appendPoint(p)
	}
	return string(b)
}

// newOrchRelHarness boots n nodes with a shared config store, at least one
// file-instance vault (the default), and real vault-ctl Raft. Additional vaults
// can be registered via options (see withExtraVault). Blocks until every
// node reports LocalVaultsReplicationReady.
func newOrchRelHarness(t *testing.T, n int, opts ...orchRelOption) *orchRelHarness {
	t.Helper()
	if n < 1 {
		t.Fatal("orch harness requires n >= 1")
	}

	// Bound intra-package harness concurrency (see orchRelHarnessSlots).
	// Registered before any other cleanup so the slot is released only after
	// full node teardown.
	orchRelHarnessSlots <- struct{}{}
	t.Cleanup(func() { <-orchRelHarnessSlots })

	sharedCtx, sharedCancel := context.WithCancel(context.Background())
	// Vault and instance share the same ID — instance ID equals vault ID.
	defaultID := glid.New()
	h := &orchRelHarness{
		t:            t,
		nodes:        make(map[string]*orchRelNode, n),
		nodeIDs:      make([]string, 0, n),
		cfgStore:     sysmem.NewStore(),
		vaultID:      defaultID,
		sharedCtx:    sharedCtx,
		sharedCancel: sharedCancel,
	}
	// The default vault (vaults[0]) is placed on every node.
	defaultIdxs := make([]int, n)
	for i := range n {
		defaultIdxs[i] = i
	}
	h.vaults = []vaultSpec{{
		label:    "A",
		id:       h.vaultID,
		nodeIdxs: defaultIdxs,
	}}

	// Apply options (e.g. additional vaults) before any state is written.
	for _, opt := range opts {
		opt(h)
	}

	// Phase 1: create cluster servers so peer addresses exist before we
	// build the NodeAddressResolver. Each node's identity is a GLID
	// (its string form) so buildVaultRaftMembers' call to
	// NodeAddressResolver(nodeID) is well-defined.
	for i := range n {
		nodeGLID := glid.New()
		id := nodeGLID.String()
		label := fmt.Sprintf("node-%d", i+1)
		h.nodeIDs = append(h.nodeIDs, id)
		node := &orchRelNode{
			id:    id,
			label: label,
			home:  filepath.Join(t.TempDir(), label),
		}
		srv, err := cluster.New(cluster.Config{ClusterAddr: "127.0.0.1:0"})
		if err != nil {
			t.Fatalf("%s: cluster.New: %v", label, err)
		}
		// Initialize multiraft transport.
		_ = srv.Transport()
		node.clusterSrv = srv
		h.nodes[id] = node
	}
	// Phase 2: start gRPC on all cluster servers so peers can dial.
	for _, id := range h.nodeIDs {
		if err := h.nodes[id].clusterSrv.Start(); err != nil {
			t.Fatalf("%s: cluster.Start: %v", id, err)
		}
	}
	t.Cleanup(func() {
		sharedCancel()
		for _, id := range h.nodeIDs {
			h.stopNode(id)
			n := h.nodes[id]
			if n != nil && n.clusterSrv != nil {
				n.clusterSrv.Stop()
				n.clusterSrv = nil
			}
		}
	})

	// Phase 3: seed shared config (vault + instance + placements). Every node
	// reads the same sysmem store so ApplyConfig produces the same view.
	h.seedSharedConfig()

	// Phase 4: wire raftwal + GroupManager + orchestrator on each node.
	for _, id := range h.nodeIDs {
		h.startNode(id)
	}

	// Phase 5: wait for vault-ctl Raft to bootstrap on every node.
	h.waitForAllReady()
	return h
}

// seedSharedConfig writes a vault, a file-backed instance, and vault placements
// (one per node, first is leader) to the shared config store. Also
// registers per-node FileStorage entries so findLocalFileStorage can
// resolve a chunk directory on each node.
func (h *orchRelHarness) seedSharedConfig() {
	h.t.Helper()
	ctx := context.Background()

	// Register every node with its canonical GLID. Also register a
	// NodeStorageConfig containing a FileStorage with a per-node chunk
	// directory — the file-instance factory requires `dir` in its params, and
	// that comes from findLocalFileStorage at ApplyConfig time.
	for _, id := range h.nodeIDs {
		nodeGLID, err := glid.Parse(id)
		if err != nil {
			h.t.Fatalf("parse node GLID %q: %v", id, err)
		}
		n := h.nodes[id]
		if err := h.cfgStore.PutNode(ctx, system.NodeConfig{
			ID:   nodeGLID,
			Name: n.label,
		}); err != nil {
			h.t.Fatalf("PutNode %s: %v", n.label, err)
		}
		storageID := glid.New()
		n.fileStorageID = storageID
		if err := h.cfgStore.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
			NodeID: id,
			FileStorages: []system.FileStorage{
				{
					ID:           storageID,
					StorageClass: harnessStorageClass,
					Name:         n.label + "-fs",
					Path:         filepath.Join(n.home, "chunks"),
				},
			},
		}); err != nil {
			h.t.Fatalf("PutNodeStorageConfig %s: %v", n.label, err)
		}
	}

	// Pipeline mode: one shared record-count rotation policy referenced by
	// every vault, so the vault-ctl leader seals the open-chunk manifest
	// after chunkMaxRecords records and homes build the sealed GLCB.
	var rotationPolicyID *glid.GLID
	if h.pipeline != nil && h.pipeline.chunkMaxRecords > 0 {
		rpID := glid.New()
		maxRecords := h.pipeline.chunkMaxRecords
		if err := h.cfgStore.PutRotationPolicy(ctx, system.RotationPolicyConfig{
			ID:         rpID,
			Name:       "orch-rel-pipeline-rotation",
			MaxRecords: &maxRecords,
		}); err != nil {
			h.t.Fatalf("PutRotationPolicy: %v", err)
		}
		rotationPolicyID = &rpID
	}
	// Keep the shared policy reachable for vaults created mid-test
	// (addRuntimeVault) so they seal on the same record-count policy.
	h.rotationPolicyID = rotationPolicyID

	// Register every vault + instance + placement. vaults[0] is the default;
	// additional entries come from withExtraVault options.
	for _, v := range h.vaults {
		if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
			ID:               v.id,
			Name:             "orch-rel-vault-" + v.label,
			Type:             system.VaultTypeFile,
			StorageClass:     harnessStorageClass,
			RotationPolicyID: rotationPolicyID,
		}); err != nil {
			h.t.Fatalf("PutVault %s: %v", v.label, err)
		}
		// Placements: one per participating node. First listed is leader.
		placements := make([]system.VaultPlacement, 0, len(v.nodeIdxs))
		for pos, idx := range v.nodeIdxs {
			if idx < 0 || idx >= len(h.nodeIDs) {
				h.t.Fatalf("vault %s: invalid node index %d (have %d nodes)", v.label, idx, len(h.nodeIDs))
			}
			n := h.nodes[h.nodeIDs[idx]]
			placements = append(placements, system.VaultPlacement{
				StorageID: n.fileStorageID.String(),
				Leader:    pos == 0,
			})
		}
		if err := h.cfgStore.SetVaultPlacements(ctx, v.id, placements); err != nil {
			h.t.Fatalf("SetVaultPlacements %s: %v", v.label, err)
		}
	}

	// Match-all routes (withMatchAllRoute): records entering the pipeline
	// routing stage on any node fan out to the targeted vault.
	for _, idx := range h.routeVaultIdxs {
		if idx < 0 || idx >= len(h.vaults) {
			h.t.Fatalf("withMatchAllRoute: invalid vault index %d (have %d vaults)", idx, len(h.vaults))
		}
		v := h.vaults[idx]
		if err := h.cfgStore.PutRoute(ctx, system.RouteConfig{
			ID:   glid.New(),
			Name: "orch-rel-route-" + v.label,
			Stages: []system.RouteStage{
				{Match: &system.MatchStage{Expression: "*"}},
			},
			Destinations: []glid.GLID{v.id},
			Enabled:      true,
		}); err != nil {
			h.t.Fatalf("PutRoute %s: %v", v.label, err)
		}
	}
}

// startNode opens raftwal, creates GroupManager, constructs orchestrator,
// applies the shared config, and starts the scheduler. Reusable after
// stopNode to exercise restart scenarios — the home directory persists
// so raftwal replays on reopen.
func (h *orchRelHarness) startNode(id string) {
	h.t.Helper()
	n := h.nodes[id]

	walDir := home.New(n.home).VaultCtlWALDir()
	wal, err := raftwal.Open(walDir)
	if err != nil {
		h.t.Fatalf("%s: raftwal.Open: %v", id, err)
	}
	n.wal = wal

	groupMgr := raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{
		Transport: n.clusterSrv.MultiRaftTransport(),
		NodeID:    id,
		BaseDir:   filepath.Join(n.home, "raft/groups"),
		WAL:       wal,
	})
	n.groupMgr = groupMgr

	logger := slog.New(slog.DiscardHandler)
	if os.Getenv("ORCH_REL_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", n.label)
	}
	orchCfg := orchestrator.Config{
		LocalNodeID: id,
		Logger:      logger,
		// Per-node segments base, mirroring production's <home>/segments.
		SegmentsDir: filepath.Join(n.home, "segments"),
	}
	if h.pipeline != nil {
		orchCfg.SegmentCompletePolicy = h.pipeline.completePolicy
		// Hot-reload paths (ReloadFilters, AddVaultInstance, placement
		// sweep) need read access to the shared config store, exactly as
		// production wires the system store.
		orchCfg.SystemLoader = h.cfgStore
	}
	orch, err := orchestrator.New(orchCfg)
	if err != nil {
		h.t.Fatalf("%s: orchestrator.New: %v", id, err)
	}
	n.orch = orch

	factories := orchestrator.Factories{
		GroupManager:        groupMgr,
		NodeAddressResolver: h.resolver(),
		ChunkManagers: map[string]chunk.ManagerFactory{
			vaultTypeKey: chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			vaultTypeKey: indexfile.NewFactory(),
		},
		Logger: logger,
	}
	// Vault-ctl multiraft always dials through PeerConns (shared with
	// ClusterService when pipeline mode is enabled).
	n.peerConns = cluster.NewStaticPeerConns(id, h.resolver())
	n.peerConns.SetStaticPeerIDs(h.nodeIDs)
	n.clusterSrv.MultiRaftTransport().SetPeerConnPool(n.peerConns)
	if h.pipeline != nil {
		factories.PeerConns = n.peerConns
		n.clusterSrv.SetSegmentPullServer(orch.ServeSegmentPull)
		n.clusterSrv.SetChunkGLCBPullServer(orch.ServeChunkGLCBPull)
		// ForwardVaultApply receiver: applies forwarded vault-ctl commands to
		// the local Raft group, mirroring wireClusterRaftApplies in app.go.
		// Without it, origin publishes from non-leader nodes are rejected
		// with "group apply function not configured" and segments never
		// reach the registry.
		n.clusterSrv.SetGroupApplyFn(func(_ context.Context, groupID string, data []byte) (uint64, error) {
			g := groupMgr.GetGroup(groupID)
			if g == nil {
				return 0, fmt.Errorf("raft group %s not found", groupID)
			}
			future := g.Raft.Apply(data, cluster.ReplicationTimeout)
			if err := future.Error(); err != nil {
				return 0, err
			}
			if resp := future.Response(); resp != nil {
				if err, ok := resp.(error); ok && err != nil {
					return future.Index(), err
				}
			}
			return future.Index(), nil
		})
	}
	n.factories = factories

	ctx := context.Background()
	sys, err := h.cfgStore.Load(ctx)
	if err != nil {
		h.t.Fatalf("%s: cfgStore.Load: %v", id, err)
	}
	if err := orch.ApplyConfig(sys, factories); err != nil {
		h.t.Fatalf("%s: ApplyConfig: %v", id, err)
	}

	runCtx, cancel := context.WithCancel(h.sharedCtx)
	n.cancel = cancel
	if err := orch.Start(runCtx); err != nil {
		h.t.Fatalf("%s: orch.Start: %v", id, err)
	}
}

// setVaultPlacements rewrites a vault's placement list to the given node
// indexes (first is the placement leader) in the shared config store. Callers
// emulate the production dispatcher fan-out afterwards (AddVaultInstance /
// RemoveVaultInstance / ReloadFilters on each node).
func (h *orchRelHarness) setVaultPlacements(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	placements := make([]system.VaultPlacement, 0, len(nodeIdxs))
	for pos, idx := range nodeIdxs {
		n := h.nodes[h.nodeIDs[idx]]
		placements = append(placements, system.VaultPlacement{
			StorageID: n.fileStorageID.String(),
			Leader:    pos == 0,
		})
	}
	if err := h.cfgStore.SetVaultPlacements(context.Background(), v.id, placements); err != nil {
		h.t.Fatalf("SetVaultPlacements %s: %v", v.label, err)
	}
}

// resolver returns a function mapping a node ID (LocalNodeID string) to
// its cluster server's bound address. Used by orchestrator to build
// vault-ctl Raft group membership.
func (h *orchRelHarness) resolver() func(string) (string, bool) {
	addrs := make(map[string]string, len(h.nodeIDs))
	for _, id := range h.nodeIDs {
		addrs[id] = h.nodes[id].clusterSrv.Addr()
	}
	return func(nodeID string) (string, bool) {
		a, ok := addrs[nodeID]
		return a, ok
	}
}

// stopNode shuts down the orchestrator, then the group manager, then the
// WAL, then the cluster server. Order matters: orchestrator owns the
// scheduler jobs that still touch instance managers; the group manager keeps
// Raft running.
func (h *orchRelHarness) stopNode(id string) {
	n, ok := h.nodes[id]
	if !ok {
		return
	}
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	if n.orch != nil {
		n.orch.Stop()
		n.orch = nil
	}
	if n.groupMgr != nil {
		n.groupMgr.Shutdown()
		n.groupMgr = nil
	}
	if n.wal != nil {
		_ = n.wal.Close()
		n.wal = nil
	}
	if n.peerConns != nil {
		_ = n.peerConns.Close()
		n.peerConns = nil
	}
}

// wipeNode removes all persistent state for a node (WAL + raft groups +
// chunk directories). The node must be stopped first; call startNode
// afterwards to bring it back up with an empty state. Simulates a
// disk-replacement scenario: the node rejoins the cluster and must
// catch up via replication from its peers. See FollowerWipe scenarios.
func (h *orchRelHarness) wipeNode(id string) {
	h.t.Helper()
	n := h.nodes[id]
	if n == nil {
		return
	}
	if n.wal != nil || n.orch != nil {
		h.t.Fatalf("wipeNode: must stopNode(%s) first", id)
	}
	raftDir := filepath.Join(n.home, "raft")
	if err := os.RemoveAll(raftDir); err != nil {
		h.t.Fatalf("wipeNode %s raft: %v", id, err)
	}
	chunkDir := filepath.Join(n.home, "chunks")
	if err := os.RemoveAll(chunkDir); err != nil {
		h.t.Fatalf("wipeNode %s chunks: %v", id, err)
	}
}

// pausePeer makes all inbound gRPC handlers on `id` block until unpausePeer
// is called. Simulates a SIGSTOPed peer: TCP stays up, application-level
// progress halts. Use to test that the rest of the cluster keeps serving
// while one peer is frozen. See gastrolog-5oofa.
func (h *orchRelHarness) pausePeer(id string) {
	h.t.Helper()
	n := h.nodes[id]
	if n == nil || n.clusterSrv == nil {
		h.t.Fatalf("pausePeer: node %s not running", id)
	}
	n.clusterSrv.Pause()
}

// unpausePeer releases a previously-paused peer so its RPC handlers resume.
func (h *orchRelHarness) unpausePeer(id string) {
	h.t.Helper()
	n := h.nodes[id]
	if n == nil || n.clusterSrv == nil {
		return
	}
	n.clusterSrv.Unpause()
}

// slowPeer adds per-handler latency to an otherwise-healthy peer.
// d=0 clears the slow-down. Use for scenarios that need slow-but-not-
// stopped behavior (e.g. verifying backoff + retry paths when a peer
// responds but misses the tight deadline).
func (h *orchRelHarness) slowPeer(id string, d time.Duration) {
	h.t.Helper()
	n := h.nodes[id]
	if n == nil || n.clusterSrv == nil {
		h.t.Fatalf("slowPeer: node %s not running", id)
	}
	n.clusterSrv.SlowDown(d)
}

// waitForAllReady blocks until every live node reports
// LocalVaultsReplicationReady == true. This is the actual gate search and
// ingest RPCs use — regressing it is what gastrolog-5j6eu fixed.
func (h *orchRelHarness) waitForAllReady() {
	h.t.Helper()
	// Progress metric: the set of not-ready nodes plus their vault-ctl Raft
	// state/term — election activity counts as progress even while readiness
	// is still false, so a slow-but-live election never reads as a stall.
	h.waitProgress("vault replication readiness", 50*time.Millisecond, func() (string, bool) {
		var notReady []string
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			if n.orch == nil {
				continue
			}
			if !n.orch.LocalVaultsReplicationReady() {
				notReady = append(notReady, fmt.Sprintf("%s(%s)", n.label, h.vaultCtlRaftView(n)))
			}
		}
		return fmt.Sprintf("not_ready=%v", notReady), len(notReady) == 0
	}, func() {
		// A stall here means a genuine hang, not a slow converge. The
		// goroutine profile distinguishes "still electing" from "wedged".
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 1)
		h.t.Logf("goroutine profile at readiness stall:\n%s", stacks.String())
	})
}

// vaultCtlRaftView summarizes a node's default-vault vault-ctl Raft group
// (state + term) for readiness/leader progress metrics.
func (h *orchRelHarness) vaultCtlRaftView(n *orchRelNode) string {
	if n == nil || n.groupMgr == nil {
		return "down"
	}
	g := n.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(h.vaultID))
	if g == nil {
		return "no-group"
	}
	return fmt.Sprintf("%s@t%s", g.Raft.State(), g.Raft.Stats()["term"])
}

// appendOnLeaderForVault appends to a specific vault's vault leader (the
// vault-ctl Raft leader for that vault, not the placement leader).
// Parameterized variant of appendOnLeader used by multi-vault tests.
func (h *orchRelHarness) appendOnLeaderForVault(v vaultSpec, rec chunk.Record) error {
	h.t.Helper()
	leader := h.waitForVaultCtlLeaderForVault(v)
	return leader.orch.AppendToVault(v.id, chunk.ChunkID{}, rec)
}

// sealOnLeaderForVault seals the active chunk for a specific vault on
// that vault's vault-ctl Raft leader.
func (h *orchRelHarness) sealOnLeaderForVault(v vaultSpec) {
	h.t.Helper()
	leader := h.waitForVaultCtlLeaderForVault(v)
	if _, err := leader.orch.SealActive(v.id); err != nil {
		h.t.Fatalf("SealActiveChunk vault %s: %v", v.label, err)
	}
}

// waitForVaultCtlLeaderForVault returns the node that currently holds
// leadership of the given vault's vault-ctl Raft group. Election activity
// (state/term changes) counts as progress; only a fully quiescent
// leaderless group reads as a stall.
func (h *orchRelHarness) waitForVaultCtlLeaderForVault(v vaultSpec) *orchRelNode {
	h.t.Helper()
	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	var leader *orchRelNode
	h.waitProgress(fmt.Sprintf("vault %s vault-ctl leader election", v.label), 30*time.Millisecond, func() (string, bool) {
		var views []string
		for _, idx := range v.nodeIdxs {
			n := h.nodes[h.nodeIDs[idx]]
			if n == nil || n.groupMgr == nil {
				views = append(views, "down")
				continue
			}
			g := n.groupMgr.GetGroup(gid)
			if g == nil {
				views = append(views, "no-group")
				continue
			}
			if g.Raft.State() == hraft.Leader {
				leader = n
				return "", true
			}
			views = append(views, fmt.Sprintf("%s@t%s", g.Raft.State(), g.Raft.Stats()["term"]))
		}
		return fmt.Sprintf("%v", views), false
	}, nil)
	return leader
}

// chunkIDsOnNodeForVault returns the chunk IDs in the given vault's
// vault-ctl FSM on `id`. Returns nil if the node doesn't host the vault.
func (h *orchRelHarness) chunkIDsOnNodeForVault(v vaultSpec, id string) map[chunk.ChunkID]bool {
	n := h.nodes[id]
	if n == nil || n.groupMgr == nil {
		return nil
	}
	g := n.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(v.id))
	if g == nil {
		return nil
	}
	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil
	}
	sub := vfsm.VaultFSM(v.id)
	if sub == nil {
		return map[chunk.ChunkID]bool{}
	}
	entries := sub.List()
	out := make(map[chunk.ChunkID]bool, len(entries))
	for _, e := range entries {
		out[e.ID] = true
	}
	return out
}

// chunkIDsOnNode returns the chunk IDs present in the vault-ctl vault-ctl FSM on
// a node. Reads the replicated metadata directly instead of via
// ListAllChunkMetas — ListAllChunkMetas overlays FSM state onto the local
// chunk-manager view, which is empty on nodes that are not the vault-ctl
// Raft leader (followers don't hold chunk files, only FSM metadata).
func (h *orchRelHarness) chunkIDsOnNode(id string) map[chunk.ChunkID]bool {
	n := h.nodes[id]
	if n == nil || n.groupMgr == nil {
		return nil
	}
	g := n.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(h.vaultID))
	if g == nil {
		return nil
	}
	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil
	}
	sub := vfsm.VaultFSM(h.vaultID)
	if sub == nil {
		return map[chunk.ChunkID]bool{}
	}
	entries := sub.List()
	out := make(map[chunk.ChunkID]bool, len(entries))
	for _, e := range entries {
		out[e.ID] = true
	}
	return out
}

// chunkIDsOnLeader returns the chunk IDs as observed by the current
// vault-ctl Raft leader. Reading from the leader avoids a flaky pattern
// where `chunkIDsOnNode(h.nodeIDs[0])` is called immediately after
// `sealOnLeader()`: SealActiveChunk only blocks on the leader's local FSM
// apply, so a non-leader at h.nodeIDs[0] can still be lagging and
// return an empty/stale set as the test's "expected".
func (h *orchRelHarness) chunkIDsOnLeader() map[chunk.ChunkID]bool {
	h.t.Helper()
	return h.chunkIDsOnNode(h.waitForVaultCtlLeader().id)
}

// assertAllNodesSee waits until every node's chunk ID set contains expected
// and no unexpected extras, or fails on stall. Multi-stage convergence (wipe
// recovery: node boot + vault-ctl snapshot install + 20s catchup-sweep ticks
// + chunk push) is fine — each stage moves the per-node counts, and every
// move resets the stall clock.
func (h *orchRelHarness) assertAllNodesSee(expected map[chunk.ChunkID]bool) {
	h.t.Helper()
	var lastSnapshot map[string]map[chunk.ChunkID]bool
	h.waitProgress("chunk-ID set convergence", 50*time.Millisecond, func() (string, bool) {
		lastSnapshot = map[string]map[chunk.ChunkID]bool{}
		converged := true
		var counts []string
		for _, id := range h.nodeIDs {
			seen := h.chunkIDsOnNode(id)
			lastSnapshot[id] = seen
			matched := 0
			for cid := range expected {
				if seen[cid] {
					matched++
				}
			}
			if len(seen) != len(expected) || matched != len(expected) {
				converged = false
			}
			counts = append(counts, fmt.Sprintf("%s=%d/%d(+%d extra)",
				h.nodes[id].label, matched, len(expected), len(seen)-matched))
		}
		return fmt.Sprintf("%v", counts), converged
	}, func() {
		expectedKeys := make([]chunk.ChunkID, 0, len(expected))
		for k := range expected {
			expectedKeys = append(expectedKeys, k)
		}
		slices.SortFunc(expectedKeys, func(a, b chunk.ChunkID) int {
			return slices.Compare(a[:], b[:])
		})
		var expHex []string
		for _, k := range expectedKeys {
			expHex = append(expHex, fmt.Sprintf("%x", k[:]))
		}
		h.t.Logf("chunk-ID sets did not converge:\nexpected (%d): %v\nactual:\n%s",
			len(expected), expHex, formatChunkSnapshot(lastSnapshot))
	})
}

func formatChunkSnapshot(m map[string]map[chunk.ChunkID]bool) string {
	var b []byte
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		b = append(b, []byte(fmt.Sprintf("=== %s ===\n", id))...)
		keys := make([]chunk.ChunkID, 0, len(m[id]))
		for k := range m[id] {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b chunk.ChunkID) int {
			for i := range a {
				if a[i] != b[i] {
					return int(a[i]) - int(b[i])
				}
			}
			return 0
		})
		for _, k := range keys {
			b = append(b, []byte(fmt.Sprintf("  %x\n", k[:]))...)
		}
	}
	return string(b)
}

// appendOnLeader appends a single record through the **vault-ctl Raft
// leader** (not the placement leader). The vault-ctl Raft group elects its
// own leader via normal Raft election; appending elsewhere would succeed
// at AppendToVault but the announcer's vault-ctl Apply would fail with
// ErrNotLeader (peerConns is nil in this harness, so no forwarder).
func (h *orchRelHarness) appendOnLeader(rec chunk.Record) error {
	h.t.Helper()
	leader := h.waitForVaultCtlLeader()
	return leader.orch.AppendToVault(h.vaultID, chunk.ChunkID{}, rec)
}

// sealOnLeader seals the active chunk on every instance of the vault, on the
// vault-ctl Raft leader. Legacy chunk-manager sealing still requires the leader;
// pipeline chunking proposes CmdSealChunk from any home via the applier forwarder.
func (h *orchRelHarness) sealOnLeader() {
	h.t.Helper()
	leader := h.waitForVaultCtlLeader()
	if _, err := leader.orch.SealActive(h.vaultID); err != nil {
		h.t.Fatalf("SealActiveChunk: %v", err)
	}
}

// waitForVaultCtlLeader waits until the default vault's vault-ctl Raft group
// has elected a leader and returns the node that currently holds leadership.
func (h *orchRelHarness) waitForVaultCtlLeader() *orchRelNode {
	h.t.Helper()
	return h.waitForVaultCtlLeaderForVault(h.vaults[0])
}
