package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// deferredGroupFixture is one node with a real raftwal-backed GroupManager
// and an orchestrator whose system loader returns a mutable cluster node list.
// peerID names a second cluster node that is never reachable.
type deferredGroupFixture struct {
	orch      *Orchestrator
	mgr       *raftgroup.GroupManager
	factories Factories
	loader    *testSystemLoaderWithRuntime
	localID   glid.GLID
	peerID    glid.GLID
	vaultCfg  system.VaultConfig
}

func newDeferredGroupFixture(t *testing.T) *deferredGroupFixture {
	t.Helper()
	localID := glid.New()
	peerID := glid.New()
	local := localID.String()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	tp := multiraft.New(
		hraft.ServerAddress(local),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	pool := multiraft.NewSimpleDialerPeerPool(map[string]func() (net.Conn, error){
		local: func() (net.Conn, error) { return lis.Dial() },
		peerID.String(): func() (net.Conn, error) {
			return nil, errors.New("peer unreachable in test")
		},
	})
	tp.SetPeerConnPool(pool)
	tp.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		pool.Close()
		srv.Stop()
		_ = tp.Close()
	})

	baseDir := t.TempDir()
	wal, err := raftwal.Open(filepath.Join(baseDir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	mgr := raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{
		Transport: tp,
		NodeID:    local,
		BaseDir:   baseDir,
		WAL:       wal,
	})
	t.Cleanup(func() { mgr.Shutdown() })

	orch := newTestOrch(t, Config{LocalNodeID: local})

	factories := Factories{
		GroupManager: mgr,
		HomeDir:      t.TempDir(),
		NodeAddressResolver: func(nodeID string) (string, bool) {
			return nodeID, true
		},
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(_ map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(_ map[string]string, _ chunk.ChunkManager, _ *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}
	if err := orch.ApplyConfig(nil, factories); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	vaultCfg := testVaultCfg(glid.New(), system.VaultTypeMemory)
	loader := &testSystemLoaderWithRuntime{cfg: &system.Config{Vaults: []system.VaultConfig{vaultCfg}}}
	orch.setSystemLoader(loader)

	return &deferredGroupFixture{
		orch:      orch,
		mgr:       mgr,
		factories: factories,
		loader:    loader,
		localID:   localID,
		peerID:    peerID,
		vaultCfg:  vaultCfg,
	}
}

func (f *deferredGroupFixture) setNodes(ids ...glid.GLID) []system.NodeConfig {
	nodes := make([]system.NodeConfig, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, system.NodeConfig{ID: id, Name: id.String()[:8]})
	}
	f.loader.mu.Lock()
	f.loader.rt.Nodes = nodes
	f.loader.mu.Unlock()
	return nodes
}

// placeLeaderOnPeer gives the vault a leader placement on the peer node, so
// the local node is a vault-ctl voter without a local instance.
func (f *deferredGroupFixture) placeLeaderOnPeer() {
	f.loader.mu.Lock()
	f.loader.rt.VaultPlacements = map[glid.GLID][]system.VaultPlacement{
		f.vaultCfg.ID: {{StorageID: system.SyntheticStoragePrefix + f.peerID.String(), Leader: true}},
	}
	f.loader.mu.Unlock()
}

// resolverHiding resolves every node except hidden.
func (f *deferredGroupFixture) resolverHiding(hidden glid.GLID) Factories {
	partial := f.factories
	resolvesAll := f.factories.NodeAddressResolver
	partial.NodeAddressResolver = func(nodeID string) (string, bool) {
		if nodeID == hidden.String() {
			return "", false
		}
		return resolvesAll(nodeID)
	}
	return partial
}

func (f *deferredGroupFixture) group() *raftgroup.Group {
	return f.mgr.GetGroup(raftgroup.VaultControlPlaneGroupID(f.vaultCfg.ID))
}

func (f *deferredGroupFixture) liveSubFSM(t *testing.T) *vaultctlfsm.FSM {
	t.Helper()
	g := f.group()
	if g == nil {
		t.Fatal("vault-ctl group does not exist on this node")
	}
	return g.FSM.(*vaultraft.FSM).EnsureVaultFSM(f.vaultCfg.ID)
}

func (f *deferredGroupFixture) vault(t *testing.T) *Vault {
	t.Helper()
	f.orch.mu.RLock()
	defer f.orch.mu.RUnlock()
	v := f.orch.vaults[f.vaultCfg.ID]
	if v == nil {
		t.Fatalf("vault %s is not registered", f.vaultCfg.ID)
	}
	return v
}

func (f *deferredGroupFixture) instance(t *testing.T) *VaultInstance {
	t.Helper()
	inst := f.vault(t).Instance
	if inst == nil {
		t.Fatalf("vault %s has no local instance", f.vaultCfg.ID)
	}
	return inst
}

func (f *deferredGroupFixture) leaderLoopRunning() bool {
	f.orch.vaultCtlLeaders.mu.Lock()
	defer f.orch.vaultCtlLeaders.mu.Unlock()
	_, ok := f.orch.vaultCtlLeaders.epochs[f.vaultCfg.ID]
	return ok
}

// pipelineEventsFSM is the sub-FSM carrying the vault's pipeline chunk-event
// subscribers, or nil.
func (f *deferredGroupFixture) pipelineEventsFSM() *vaultctlfsm.FSM {
	v, ok := f.orch.vaultCtlPipelineChunkEvents.Load(f.vaultCfg.ID)
	if !ok {
		return nil
	}
	return v.(*vaultctlfsm.FSM)
}

// assertGroupBound checks the vault-level bindings every node makes once the
// group exists, local instance or not.
func assertGroupBound(t *testing.T, f *deferredGroupFixture) {
	t.Helper()
	live := f.liveSubFSM(t)
	if !f.leaderLoopRunning() {
		t.Error("vault-ctl leader loop not started: this node never reconciles desired membership when it leads the group")
	}
	if f.pipelineEventsFSM() != live {
		t.Error("pipeline chunk events not wired to the live sub-FSM: the inspector misses active and sealing chunks")
	}
}

// assertInstanceWiredToGroup checks every binding an instance gets when it
// meets its vault-ctl group.
func assertInstanceWiredToGroup(t *testing.T, f *deferredGroupFixture) {
	t.Helper()
	assertGroupBound(t, f)
	live := f.liveSubFSM(t)
	inst := f.instance(t)
	if inst.IsFSMReady == nil {
		t.Error("IsFSMReady is nil: ReadinessErr reports the vault ready without consulting the vault-ctl group")
	}
	if inst.vaultCtl != live {
		t.Error("instance not bound to the live sub-FSM: OnCreate/OnUpload and the cloud-backed resolver read nothing")
	}
	if inst.ApplyRaftAckDelete == nil {
		t.Error("ApplyRaftAckDelete is nil: this node cannot ack a retention delete it is listed in ExpectedFrom for")
	}
	if inst.ManifestEntries == nil {
		t.Error("ManifestEntries is nil: manifest reads fall back to the local chunk manager")
	}
	if inst.Reconciler == nil || inst.Reconciler.fsm.Load() != live {
		t.Error("lifecycle reconciler not wired to the live vault-ctl sub-FSM: onRequestDelete/onSeal/onFinalizeDelete never fire")
	}
}

// Control: the group is creatable when the instance is built, so every
// binding lands. Proves the assertions are satisfiable.
func TestVaultInstanceBuiltWithGroupIsWired(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	f.setNodes(f.localID)

	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}
	assertInstanceWiredToGroup(t, f)
}

// A joiner registers a vault (no placements yet, so it builds a leader
// instance) before its own NodeConfig is in the cluster node list: group
// creation defers. Its NodeConfig then lands and RefreshVaultCtlMembers
// creates the group. The instance must end up bound to that group.
func TestVaultInstanceBuiltDuringDeferredGroupIsRewiredWhenGroupAppears(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)

	f.setNodes(f.peerID) // local node not yet in the cluster node list
	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}
	if f.group() != nil {
		t.Fatal("precondition: group creation should have deferred")
	}
	if err := f.vault(t).ReadinessErr(); !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("instance whose group is pending: ReadinessErr = %v, want ErrVaultNotReady", err)
	}

	nodes := f.setNodes(f.peerID, f.localID)
	f.orch.RefreshVaultCtlMembers(nodes, f.factories)

	assertInstanceWiredToGroup(t, f)
}

// Same, with deferral caused by a peer that is in the node list but not yet
// resolvable in the cluster-ctl Raft configuration.
func TestVaultInstanceBuiltWhilePeerUnresolvableIsRewiredWhenGroupAppears(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)

	nodes := f.setNodes(f.localID, f.peerID)
	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.resolverHiding(f.peerID)); err != nil {
		t.Fatalf("AddVault: %v", err)
	}
	if f.group() != nil {
		t.Fatal("precondition: group creation should have deferred")
	}

	f.orch.RefreshVaultCtlMembers(nodes, f.factories)

	assertInstanceWiredToGroup(t, f)
}

// A node without a storage placement is still a vault-ctl voter: when the
// group appears it must run the leader loop and the vault-level bindings
// even though there is no instance to bind.
func TestVaultCtlGroupCreatedLaterForVaultWithoutLocalInstanceStartsLeaderLoop(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	f.placeLeaderOnPeer()

	f.setNodes(f.peerID)
	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}
	if f.vault(t).Instance != nil {
		t.Fatal("precondition: the vault has no local instance on a node without a placement")
	}
	if f.group() != nil || f.leaderLoopRunning() {
		t.Fatal("precondition: group creation should have deferred, and with it the leader loop")
	}

	nodes := f.setNodes(f.peerID, f.localID)
	f.orch.RefreshVaultCtlMembers(nodes, f.factories)

	assertGroupBound(t, f)
}

// Every path that meets a group — refresh, a second refresh, instance
// registration and snapshot restore — goes through the leader manager's
// Start. Two loops on one group would split LeaderCh between them, so the
// loop started when the group was created must be the only one, for a vault
// with and without a local instance.
func TestVaultCtlLeaderLoopStartsOnceAcrossBindPaths(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		localInstance bool
	}{
		{"with local instance", true},
		{"without local instance", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newDeferredGroupFixture(t)
			if !tc.localInstance {
				f.placeLeaderOnPeer()
			}
			f.setNodes(f.peerID)
			if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.factories); err != nil {
				t.Fatalf("AddVault: %v", err)
			}
			nodes := f.setNodes(f.peerID, f.localID)
			f.orch.RefreshVaultCtlMembers(nodes, f.factories)
			if !f.leaderLoopRunning() {
				t.Fatal("precondition: the refresh that created the group started the leader loop")
			}

			// Tag the running loop's epoch so a replacement is observable.
			m := f.orch.vaultCtlLeaders
			original := make(chan struct{})
			m.mu.Lock()
			cancelOriginal := m.epochs[f.vaultCfg.ID]
			m.epochs[f.vaultCfg.ID] = func() {
				close(original)
				cancelOriginal()
			}
			m.mu.Unlock()

			f.orch.RefreshVaultCtlMembers(nodes, f.factories)
			if err := f.orch.AddVaultInstance(context.Background(), f.vaultCfg.ID, f.factories); err != nil {
				t.Fatalf("AddVaultInstance: %v", err)
			}
			f.orch.runAfterVaultCtlRestore(f.vaultCfg.ID)

			m.Stop(f.vaultCfg.ID)
			select {
			case <-original:
			default:
				t.Fatal("a bind path replaced the leader loop started at group creation: two loops ran on one group")
			}
		})
	}
}

// Readiness distinguishes "no vault-ctl Raft on this node" (the instance's
// own chunk manager is authoritative: ready) from "group not created yet"
// (the replicated manifest is unavailable: not ready, until the group exists
// and has applied).
func TestVaultReadinessWhileVaultCtlGroupPending(t *testing.T) {
	t.Parallel()

	t.Run("pending group reports not ready until the group applies", func(t *testing.T) {
		t.Parallel()
		f := newDeferredGroupFixture(t)
		nodes := f.setNodes(f.localID)
		if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.resolverHiding(f.localID)); err != nil {
			t.Fatalf("AddVault: %v", err)
		}
		if f.group() != nil {
			t.Fatal("precondition: group creation should have deferred")
		}
		if err := f.vault(t).ReadinessErr(); !errors.Is(err, ErrVaultNotReady) {
			t.Fatalf("ReadinessErr with the group pending = %v, want ErrVaultNotReady", err)
		}
		if f.orch.liveReplicationReady() {
			t.Fatal("node reports replication-ready while a local instance's vault-ctl group does not exist")
		}

		f.orch.RefreshVaultCtlMembers(nodes, f.factories)
		g := f.group()
		if g == nil {
			t.Fatal("refresh with every node resolvable did not create the group")
		}
		waittest.Progress(t, "single-member vault-ctl group applies and the vault reports ready", func() (string, bool) {
			st := g.Raft.Stats()
			return fmt.Sprintf("state=%s applied=%s", st["state"], st["applied_index"]), f.vault(t).ReadinessErr() == nil
		})
		if !f.orch.liveReplicationReady() {
			t.Fatal("node not replication-ready after its only vault's group applied")
		}
	})

	t.Run("no node address resolver means no vault-ctl Raft", func(t *testing.T) {
		t.Parallel()
		f := newDeferredGroupFixture(t)
		noResolver := f.factories
		noResolver.NodeAddressResolver = nil
		orch := newTestOrch(t, Config{LocalNodeID: f.localID.String()})
		if err := orch.ApplyConfig(nil, noResolver); err != nil {
			t.Fatalf("ApplyConfig: %v", err)
		}
		orch.setSystemLoader(f.loader)
		f.setNodes(f.localID)
		if err := orch.AddVault(context.Background(), f.vaultCfg, noResolver); err != nil {
			t.Fatalf("AddVault: %v", err)
		}
		orch.mu.RLock()
		v := orch.vaults[f.vaultCfg.ID]
		orch.mu.RUnlock()
		if v == nil || v.Instance == nil {
			t.Fatal("precondition: the vault has a local instance")
		}
		if err := v.ReadinessErr(); err != nil {
			t.Fatalf("instance on a node without vault-ctl Raft: ReadinessErr = %v, want ready", err)
		}
	})
}

// A snapshot Restore replaces the sub-FSM; the instance and the vault-level
// bindings must follow it to the replacement, through the same binding the
// other paths use.
func TestVaultCtlSnapshotRestoreRebindsInstanceAndGroup(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	f.setNodes(f.localID)
	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}
	before := f.liveSubFSM(t)
	if f.instance(t).vaultCtl != before {
		t.Fatal("precondition: the instance is bound to the group's sub-FSM")
	}

	source := vaultraft.NewFSM()
	chunkID := chunk.NewChunkID()
	now := time.Now()
	if got := source.Apply(&hraft.Log{Index: 1, Data: vaultraft.MarshalVaultChunkCommand(
		f.vaultCfg.ID, vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now))}); got != nil {
		t.Fatalf("apply create chunk to snapshot source: %v", got)
	}
	snap, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&restoreBufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	if err := f.group().FSM.(*vaultraft.FSM).Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	waittest.Progress(t, "after-restore pass completes", func() (string, bool) {
		_, pending := f.orch.ctlRestorePending.Load(f.vaultCfg.ID)
		return fmt.Sprintf("pending=%v", pending), !pending
	})

	after := f.liveSubFSM(t)
	if after == before {
		t.Fatal("precondition: Restore replaced the sub-FSM")
	}
	assertInstanceWiredToGroup(t, f)
	inst := f.instance(t)
	if inst.vaultCtl != after {
		t.Fatal("instance still bound to the pre-restore sub-FSM")
	}
	if _, ok := inst.ManifestEntry(chunkID); !ok {
		t.Fatal("manifest reads do not see the restored chunk: callbacks still read the pre-restore sub-FSM")
	}
}

// restoreBufSink adapts an io.Writer to hraft.SnapshotSink.
type restoreBufSink struct{ io.Writer }

func (s *restoreBufSink) Close() error  { return nil }
func (s *restoreBufSink) ID() string    { return "test" }
func (s *restoreBufSink) Cancel() error { return nil }
