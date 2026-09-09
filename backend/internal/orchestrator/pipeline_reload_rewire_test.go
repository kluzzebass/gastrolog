package orchestrator

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"gastrolog/internal/glid"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
)

// singleNodeVaultCtlGroup gives an orchestrator a live vault-ctl handle for
// vaultID the way every cluster node has one: the control-plane Raft group
// spans all nodes whether or not they chunk for the vault.
func singleNodeVaultCtlGroup(t *testing.T, nodeID string, vaultID glid.GLID) *raftgroup.GroupManager {
	t.Helper()
	lis := bufconn.Listen(afterRestoreTestBufSize)
	srv := grpc.NewServer()
	tp := multiraft.New(hraft.ServerAddress(nodeID),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) })
	pool := multiraft.NewSimpleDialerPeerPool(map[string]func() (net.Conn, error){
		nodeID: func() (net.Conn, error) { return lis.Dial() },
	})
	tp.SetPeerConnPool(pool)
	tp.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { pool.Close(); srv.Stop(); _ = tp.Close() })

	baseDir := t.TempDir()
	wal, err := raftwal.Open(filepath.Join(baseDir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	mgr := raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{Transport: tp, NodeID: nodeID, BaseDir: baseDir, WAL: wal})
	t.Cleanup(func() { mgr.Shutdown() })

	hb, el, ll := afterRestoreRaftTimeouts()
	g, err := mgr.CreateGroup(raftgroup.GroupConfig{
		GroupID:            raftgroup.VaultControlPlaneGroupID(vaultID),
		FSM:                vaultraft.NewFSM(),
		SeedMembers:        []hraft.Server{{ID: hraft.ServerID(nodeID), Address: hraft.ServerAddress(nodeID)}},
		HeartbeatTimeout:   hb,
		ElectionTimeout:    el,
		LeaderLeaseTimeout: ll,
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	waitForRaftLeader(t, g, 5*time.Second)
	return mgr
}

// A node that does not chunk for a vault has no chunking registration to
// rewire after a config reload; the reload must not report that as a failure.
func TestConfigReloadDoesNotWarnAboutRewiringVaultsThisNodeDoesNotChunk(t *testing.T) {
	var logs bytes.Buffer
	vaultID := glid.New()
	// The vault is a route destination, so every node registers it with the
	// pipeline; with no placements the leader resolves to no node, so this
	// node is neither home nor follower and chunking never registers it.
	sys := &system.System{Config: system.Config{
		Vaults: []system.VaultConfig{{ID: vaultID, Name: "elsewhere", Type: system.VaultTypeFile, Enabled: true}},
		Routes: []system.RouteConfig{{
			ID: glid.New(), Name: "all", Priority: 10, Enabled: true,
			Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
			Destinations: []glid.GLID{vaultID},
		}},
	}}
	orch := newTestOrch(t, Config{
		LocalNodeID:  "node-local",
		SystemLoader: &staticSystemLoader{sys: sys},
		Logger:       slog.New(slog.NewTextHandler(&logs, nil)),
	})
	// With a vault-ctl handle present the rewire is attempted; without one it
	// returns early and the reload could not have warned in the first place.
	orch.groupMgr = singleNodeVaultCtlGroup(t, "node-local", vaultID)

	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if _, registered := orch.lookupPipelineVault(vaultID); !registered {
		t.Fatal("premise: the reload registers the vault with the pipeline even where it is not chunked")
	}
	if strings.Contains(logs.String(), "pipeline rewire after config reload failed") {
		t.Fatalf("reload warned about rewiring a vault this node does not chunk:\n%s", logs.String())
	}
}
