package orchestrator_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
	// vaultID/tierID are the default (first) vault's identifiers; kept as
	// top-level fields for the single-vault convenience API.
	vaultID glid.GLID
	tierID  glid.GLID
	// vaults holds every configured vault, with the default vault as
	// vaults[0]. Multi-vault scenarios use addVaultSpec during setup to
	// add more, each with its own node subset.
	vaults       []vaultSpec
	sharedCtx    context.Context
	sharedCancel context.CancelFunc
}

// vaultSpec identifies one vault in the harness along with which nodes
// participate in its tier-Raft group. First node in nodeIdxs is the
// placement leader. For multi-vault scenarios, use orchRelOptions to
// register additional vaultSpecs before startup.
type vaultSpec struct {
	label    string    // human label for test output ("A", "B", ...)
	id       glid.GLID // vault GLID
	tierID   glid.GLID // tier GLID
	nodeIdxs []int     // indexes into h.nodeIDs; first is tier leader
}

// orchRelOption configures a harness before it boots. Applied between
// nodeID assignment and cfgStore seeding, so options can influence what
// gets written to the config store.
type orchRelOption func(*orchRelHarness)

// withExtraVault registers an additional vault placed on the given
// node indexes (into h.nodeIDs). The first index is the tier leader.
// len(nodeIdxs) must be an odd number >= 1 for valid Raft quorum, and
// each index must be a valid h.nodeIDs index. The vault is labeled
// "B" (or "C", "D", ...) based on insertion order.
func withExtraVault(nodeIdxs []int) orchRelOption {
	return func(h *orchRelHarness) {
		label := string(rune('B' + len(h.vaults) - 1))
		h.vaults = append(h.vaults, vaultSpec{
			label:    label,
			id:       glid.New(),
			tierID:   glid.New(),
			nodeIdxs: nodeIdxs,
		})
	}
}

const (
	orchHarnessReadyWait  = 8 * time.Second
	orchHarnessConvWait   = 60 * time.Second
	orchHarnessLeaderWait = 5 * time.Second
)

// newOrchRelHarness boots n nodes with a shared config store, at least one
// file-tier vault (the default), and real vault-ctl Raft. Additional vaults
// can be registered via options (see withExtraVault). Blocks until every
// node reports LocalVaultsReplicationReady.
func newOrchRelHarness(t *testing.T, n int, opts ...orchRelOption) *orchRelHarness {
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
	// The default vault (vaults[0]) is placed on every node.
	defaultIdxs := make([]int, n)
	for i := range n {
		defaultIdxs[i] = i
	}
	h.vaults = []vaultSpec{{
		label:    "A",
		id:       h.vaultID,
		tierID:   h.tierID,
		nodeIdxs: defaultIdxs,
	}}

	// Apply options (e.g. additional vaults) before any state is written.
	for _, opt := range opts {
		opt(h)
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

	// Register every vault + tier + placement. vaults[0] is the default;
	// additional entries come from withExtraVault options.
	for _, v := range h.vaults {
		if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
			ID:   v.id,
			Name: "orch-rel-vault-" + v.label,
		}); err != nil {
			h.t.Fatalf("PutVault %s: %v", v.label, err)
		}
		if err := h.cfgStore.PutTier(ctx, system.TierConfig{
			ID:           v.tierID,
			Name:         "orch-rel-tier-" + v.label,
			Type:         system.TierTypeFile,
			VaultID:      v.id,
			Position:     0,
			StorageClass: harnessStorageClass,
		}); err != nil {
			h.t.Fatalf("PutTier %s: %v", v.label, err)
		}
		// Placements: one per participating node. First listed is leader.
		placements := make([]system.TierPlacement, 0, len(v.nodeIdxs))
		for pos, idx := range v.nodeIdxs {
			if idx < 0 || idx >= len(h.nodeIDs) {
				h.t.Fatalf("vault %s: invalid node index %d (have %d nodes)", v.label, idx, len(h.nodeIDs))
			}
			n := h.nodes[h.nodeIDs[idx]]
			placements = append(placements, system.TierPlacement{
				StorageID: n.fileStorageID.String(),
				Leader:    pos == 0,
			})
		}
		if err := h.cfgStore.SetTierPlacements(ctx, v.tierID, placements); err != nil {
			h.t.Fatalf("SetTierPlacements %s: %v", v.label, err)
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

// appendOnLeaderForVault appends to a specific vault's tier leader (the
// vault-ctl Raft leader for that vault, not the placement leader).
// Parameterized variant of appendOnLeader used by multi-vault tests.
func (h *orchRelHarness) appendOnLeaderForVault(v vaultSpec, rec chunk.Record) error {
	h.t.Helper()
	leader := h.waitForVaultCtlLeaderForVault(v)
	return leader.orch.AppendToTier(v.id, v.tierID, chunk.ChunkID{}, rec)
}

// sealOnLeaderForVault seals the active chunk for a specific vault on
// that vault's vault-ctl Raft leader.
func (h *orchRelHarness) sealOnLeaderForVault(v vaultSpec) {
	h.t.Helper()
	leader := h.waitForVaultCtlLeaderForVault(v)
	if _, err := leader.orch.SealActive(v.id, glid.Nil); err != nil {
		h.t.Fatalf("SealActive vault %s: %v", v.label, err)
	}
}

// waitForVaultCtlLeaderForVault returns the node that currently holds
// leadership of the given vault's vault-ctl Raft group.
func (h *orchRelHarness) waitForVaultCtlLeaderForVault(v vaultSpec) *orchRelNode {
	h.t.Helper()
	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	deadline := time.Now().Add(orchHarnessLeaderWait)
	for time.Now().Before(deadline) {
		for _, idx := range v.nodeIdxs {
			n := h.nodes[h.nodeIDs[idx]]
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
	h.t.Fatalf("vault %s: no leader within %s", v.label, orchHarnessLeaderWait)
	return nil
}

// chunkIDsOnNodeForVault returns the chunk IDs in the given vault's
// tier FSM on `id`. Returns nil if the node doesn't host the vault.
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
	sub := vfsm.TierFSM(v.tierID)
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

// chunkIDsOnLeader returns the chunk IDs as observed by the current
// vault-ctl Raft leader. Reading from the leader avoids a flaky pattern
// where `chunkIDsOnNode(h.nodeIDs[0])` is called immediately after
// `sealOnLeader()`: SealActive only blocks on the leader's local FSM
// apply, so a non-leader at h.nodeIDs[0] can still be lagging and
// return an empty/stale set as the test's "expected".
func (h *orchRelHarness) chunkIDsOnLeader() map[chunk.ChunkID]bool {
	h.t.Helper()
	return h.chunkIDsOnNode(h.waitForVaultCtlLeader().id)
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
	h.t.Fatalf("chunk-ID sets did not converge within %s:\nexpected (%d): %v\nactual:\n%s",
		orchHarnessConvWait, len(expected), expHex, formatChunkSnapshot(lastSnapshot))
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

