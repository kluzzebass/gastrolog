package orchestrator_test

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/vaultraft"

	hraft "github.com/hashicorp/raft"
)

// tierTypeKey is the string form of the file-tier type used as a
// factory-map key. File tier is used (rather than memory tier) because
// only the file-tier chunk Manager implements SetAnnouncer — the pathway
// that propagates chunk metadata events through vault-ctl Raft to
// followers. Without announcements, replication tests are vacuous.
const tierTypeKey = string(system.TierTypeFile)

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
// on. buildTierRaftMembers converts NodeConfig.ID (a GLID) to its string
// form and calls NodeAddressResolver with that, so id here MUST be the
// GLID string form, not a human-readable label.
type orchRelNode struct {
	id            string    // GLID string; also orchestrator LocalNodeID
	label         string    // human label for test output ("node-1" etc.)
	home          string
	fileStorageID glid.GLID // FileStorage.ID for this node's chunk directory
	clusterSrv    *cluster.Server
	wal           *raftwal.WAL
	groupMgr      *raftgroup.GroupManager
	orch          *orchestrator.Orchestrator
	cancel        context.CancelFunc
}

// orchRelHarness boots N in-process nodes, each running a real orchestrator
// with a vault-ctl Raft group replicated across all nodes. Unlike the
// lower-level reliability harness in backend/internal/vaultraft, this one
// exercises the full orchestrator wiring: ApplyConfig, AppendToTier,
// ListAllChunkMetas, the scheduler, vault readiness gating, and the
// vault-ctl Raft group built via createTierRaftGroupVaultCtl.
//
// Cross-node cluster RPCs (RecordForwarder, RemoteTransferrer,
// TierReplicator) are left nil — scenarios that need them should stub via
// direct in-process shims. The default harness exercises replication
// through vault-ctl Raft only, which is the primary target for
// metadata-divergence tests.
type orchRelHarness struct {
	t            *testing.T
	nodes        map[string]*orchRelNode
	nodeIDs      []string
	cfgStore     system.Store
	vaultID      glid.GLID
	tierID       glid.GLID
	sharedCtx    context.Context
	sharedCancel context.CancelFunc
}

const (
	orchHarnessReadyWait  = 8 * time.Second
	orchHarnessConvWait   = 8 * time.Second
	orchHarnessLeaderWait = 5 * time.Second
)

// newOrchRelHarness boots n nodes with a shared config store, a single
// memory-tier vault placed on every node, and real vault-ctl Raft. Blocks
// until every node reports LocalVaultsReplicationReady.
func newOrchRelHarness(t *testing.T, n int) *orchRelHarness {
	t.Helper()
	if n < 1 {
		t.Fatal("orch harness requires n >= 1")
	}

	sharedCtx, sharedCancel := context.WithCancel(context.Background())
	h := &orchRelHarness{
		t:            t,
		nodes:        make(map[string]*orchRelNode, n),
		nodeIDs:      make([]string, 0, n),
		cfgStore:     sysmem.NewStore(),
		vaultID:      glid.New(),
		tierID:       glid.New(),
		sharedCtx:    sharedCtx,
		sharedCancel: sharedCancel,
	}

	// Phase 1: create cluster servers so peer addresses exist before we
	// build the NodeAddressResolver. Each node's identity is a GLID
	// (its string form) so buildTierRaftMembers' call to
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

	// Phase 3: seed shared config (vault + tier + placements). Every node
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

// seedSharedConfig writes a vault, a file-backed tier, and tier placements
// (one per node, first is leader) to the shared config store. Also
// registers per-node FileStorage entries so findLocalFileStorage can
// resolve a chunk directory on each node.
func (h *orchRelHarness) seedSharedConfig() {
	h.t.Helper()
	ctx := context.Background()

	// Register every node with its canonical GLID. Also register a
	// NodeStorageConfig containing a FileStorage with a per-node chunk
	// directory — the file-tier factory requires `dir` in its params, and
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

	if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
		ID:   h.vaultID,
		Name: "orch-rel-vault",
	}); err != nil {
		h.t.Fatalf("PutVault: %v", err)
	}
	if err := h.cfgStore.PutTier(ctx, system.TierConfig{
		ID:           h.tierID,
		Name:         "orch-rel-tier",
		Type:         system.TierTypeFile,
		VaultID:      h.vaultID,
		Position:     0,
		StorageClass: harnessStorageClass,
	}); err != nil {
		h.t.Fatalf("PutTier: %v", err)
	}

	// All nodes are voters for this tier. The first is leader.
	placements := make([]system.TierPlacement, len(h.nodeIDs))
	for i, id := range h.nodeIDs {
		n := h.nodes[id]
		placements[i] = system.TierPlacement{
			StorageID: n.fileStorageID.String(),
			Leader:    i == 0,
		}
	}
	if err := h.cfgStore.SetTierPlacements(ctx, h.tierID, placements); err != nil {
		h.t.Fatalf("SetTierPlacements: %v", err)
	}
}

// startNode opens raftwal, creates GroupManager, constructs orchestrator,
// applies the shared config, and starts the scheduler. Reusable after
// stopNode to exercise restart scenarios — the home directory persists
// so raftwal replays on reopen.
func (h *orchRelHarness) startNode(id string) {
	h.t.Helper()
	n := h.nodes[id]

	walDir := filepath.Join(n.home, "raft/wal")
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
	orch, err := orchestrator.New(orchestrator.Config{
		LocalNodeID: id,
		Logger:      logger,
	})
	if err != nil {
		h.t.Fatalf("%s: orchestrator.New: %v", id, err)
	}
	n.orch = orch

	factories := orchestrator.Factories{
		GroupManager:        groupMgr,
		NodeAddressResolver: h.resolver(),
		ChunkManagers: map[string]chunk.ManagerFactory{
			tierTypeKey: chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			tierTypeKey: indexfile.NewFactory(),
		},
		Logger: logger,
	}

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
// scheduler jobs that still touch tier managers; the group manager keeps
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
}

// waitForAllReady blocks until every live node reports
// LocalVaultsReplicationReady == true. This is the actual gate search and
// ingest RPCs use — regressing it is what gastrolog-5j6eu fixed.
func (h *orchRelHarness) waitForAllReady() {
	h.t.Helper()
	deadline := time.Now().Add(orchHarnessReadyWait)
	var notReady []string
	for time.Now().Before(deadline) {
		notReady = notReady[:0]
		allReady := true
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			if n.orch == nil {
				continue
			}
			if !n.orch.LocalVaultsReplicationReady() {
				notReady = append(notReady, id)
				allReady = false
			}
		}
		if allReady {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("vaults not ready after %s on: %v", orchHarnessReadyWait, notReady)
}

// chunkIDsOnNode returns the chunk IDs present in the vault-ctl tier FSM on
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
	sub := vfsm.TierFSM(h.tierID)
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

// assertAllNodesSee polls until every node's chunk ID set contains
// expected and no unexpected extras, or fails.
func (h *orchRelHarness) assertAllNodesSee(expected map[chunk.ChunkID]bool) {
	h.t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	var lastSnapshot map[string]map[chunk.ChunkID]bool
	for time.Now().Before(deadline) {
		lastSnapshot = map[string]map[chunk.ChunkID]bool{}
		converged := true
		for _, id := range h.nodeIDs {
			seen := h.chunkIDsOnNode(id)
			lastSnapshot[id] = seen
			if len(seen) != len(expected) {
				converged = false
				continue
			}
			for cid := range expected {
				if !seen[cid] {
					converged = false
					break
				}
			}
		}
		if converged {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("chunk-ID sets did not converge within %s:\n%s",
		orchHarnessConvWait, formatChunkSnapshot(lastSnapshot))
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
// at AppendToTier but the announcer's vault-ctl Apply would fail with
// ErrNotLeader (peerConns is nil in this harness, so no forwarder).
func (h *orchRelHarness) appendOnLeader(rec chunk.Record) error {
	h.t.Helper()
	leader := h.waitForVaultCtlLeader()
	return leader.orch.AppendToTier(h.vaultID, h.tierID, chunk.ChunkID{}, rec)
}

// sealOnLeader seals the active chunk on every tier of the vault, on the
// vault-ctl Raft leader. Sealing on a non-leader would skip the CmdSealChunk
// announcement.
func (h *orchRelHarness) sealOnLeader() {
	h.t.Helper()
	leader := h.waitForVaultCtlLeader()
	if _, err := leader.orch.SealActive(h.vaultID, glid.Nil); err != nil {
		h.t.Fatalf("SealActive: %v", err)
	}
}

// waitForVaultCtlLeader polls until the vault-ctl Raft group has elected a
// leader and returns the node that currently holds leadership.
func (h *orchRelHarness) waitForVaultCtlLeader() *orchRelNode {
	h.t.Helper()
	gid := raftgroup.VaultControlPlaneGroupID(h.vaultID)
	deadline := time.Now().Add(orchHarnessLeaderWait)
	for time.Now().Before(deadline) {
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			if n == nil || n.groupMgr == nil {
				continue
			}
			g := n.groupMgr.GetGroup(gid)
			if g == nil {
				continue
			}
			if g.Raft.State() == hraft.Leader {
				return n
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	h.t.Fatalf("no vault-ctl Raft leader within %s", orchHarnessLeaderWait)
	return nil
}

