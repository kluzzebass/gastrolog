package orchestrator

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const afterRestoreTestBufSize = 1 << 20

func afterRestoreRaftTimeouts() (heartbeat, election, lease time.Duration) {
	return 100 * time.Millisecond, 100 * time.Millisecond, 50 * time.Millisecond
}

// TestCreateGroupRestoreWithInstanceDoesNotDeadlock pins the rule that
// afterVaultCtlRestore must not call groupMgr.GetGroup synchronously from
// inside fsm.Restore while CreateGroup holds groupMgr.mu.
func TestCreateGroupRestoreWithInstanceDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	const nodeID = "node-1"
	lis := bufconn.Listen(afterRestoreTestBufSize)
	srv := grpc.NewServer()
	tp := multiraft.New(
		hraft.ServerAddress(nodeID),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	pool := multiraft.NewSimpleDialerPeerPool(map[string]func() (net.Conn, error){
		nodeID: func() (net.Conn, error) { return lis.Dial() },
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
		NodeID:    nodeID,
		BaseDir:   baseDir,
		WAL:       wal,
	})
	t.Cleanup(func() { mgr.Shutdown() })

	vaultID := glid.New()
	groupID := raftgroup.VaultControlPlaneGroupID(vaultID)
	hb, el, ll := afterRestoreRaftTimeouts()
	seed := []hraft.Server{{
		ID:      hraft.ServerID(nodeID),
		Address: hraft.ServerAddress(nodeID),
	}}

	// Bootstrap the group and persist a snapshot on disk.
	bootstrapFSM := vaultraft.NewFSM()
	g1, err := mgr.CreateGroup(raftgroup.GroupConfig{
		GroupID:            groupID,
		FSM:                bootstrapFSM,
		SeedMembers:        seed,
		HeartbeatTimeout:   hb,
		ElectionTimeout:    el,
		LeaderLeaseTimeout: ll,
	})
	if err != nil {
		t.Fatalf("bootstrap CreateGroup: %v", err)
	}
	waitForRaftLeader(t, g1)

	now := time.Now().UTC()
	chunkID := chunk.ChunkID(glid.New())
	cmd := vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now)
	fut := g1.Raft.Apply(vaultraft.MarshalVaultChunkCommand(vaultID, cmd), 5*time.Second)
	if err := fut.Error(); err != nil {
		t.Fatalf("Apply create chunk: %v", err)
	}
	snapFut := g1.Raft.Snapshot()
	if err := snapFut.Error(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := mgr.DestroyGroup(groupID); err != nil {
		t.Fatalf("DestroyGroup: %v", err)
	}

	// Instance exists before restore — the race that deadlocked node3.
	orch, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.groupMgr = mgr
	s := memtest.MustNewVault(t, chunkmem.Config{})
	orch.RegisterVault(NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	restoreFSM := vaultraft.NewFSM()
	restoreFSM.SetOnAfterRestore(func() { orch.afterVaultCtlRestore(vaultID) })

	done := make(chan error, 1)
	go func() {
		_, err := mgr.CreateGroup(raftgroup.GroupConfig{
			GroupID:            groupID,
			FSM:                restoreFSM,
			SeedMembers:        seed,
			HeartbeatTimeout:   hb,
			ElectionTimeout:    el,
			LeaderLeaseTimeout: ll,
		})
		done <- err
	}()

	if err := waittest.Recv(t, "restore CreateGroup with a registered instance returns", done, nil); err != nil {
		t.Fatalf("restore CreateGroup: %v", err)
	}

	waittest.Progress(t, "deferred after-restore pass completes", func() (string, bool) {
		_, pending := orch.ctlRestorePending.Load(vaultID)
		return fmt.Sprintf("pending=%v", pending), !pending
	})
}

// waitForRaftLeader waits for g to elect a leader, failing when raft state,
// term and log index stop changing.
func waitForRaftLeader(t *testing.T, g *raftgroup.Group) {
	t.Helper()
	waittest.Progress(t, "raft group elects a leader", func() (string, bool) {
		st := g.Raft.Stats()
		return fmt.Sprintf("state=%s term=%s last_log=%s", st["state"], st["term"], st["last_log_index"]), g.Raft.Leader() != ""
	})
}
