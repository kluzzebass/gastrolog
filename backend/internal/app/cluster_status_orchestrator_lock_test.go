package app

// Cluster status is what an operator reaches for when a node misbehaves, so
// it must answer while the orchestrator is stuck. A vault teardown holds the
// orchestrator's lock across calls into the vault's storage and its Raft
// group; when one of those never returns, everything that takes the lock
// waits with it.

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"
)

// stuckChunkManager, once armed, does not return from List, the way a vault
// teardown blocks on storage or a Raft group that will not answer.
type stuckChunkManager struct {
	chunk.ChunkManager
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *stuckChunkManager) List() ([]chunk.ChunkMeta, error) {
	if m.armed.Load() {
		m.once.Do(func() { close(m.entered) })
		<-m.release
	}
	return m.ChunkManager.List()
}

// oneNodeCluster is the cluster status provider of a node that leads a
// configuration of one.
type oneNodeCluster struct{ id string }

func (c oneNodeCluster) LeaderInfo() (string, string) { return c.id + ":4566", c.id }
func (c oneNodeCluster) Servers() ([]cluster.RaftServer, error) {
	return []cluster.RaftServer{{ID: c.id, Address: c.id + ":4566", Suffrage: "Voter"}}, nil
}
func (c oneNodeCluster) LocalStats() map[string]string { return nil }
func (c oneNodeCluster) IsLeader() bool                { return true }
func (c oneNodeCluster) LeadershipTransfer() error     { return nil }

func TestClusterStatusAnswersWhileTheOrchestratorLockIsHeld(t *testing.T) {
	t.Parallel()
	const nodeID = "node-a"
	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}
	v := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(1000)})
	stuck := &stuckChunkManager{ChunkManager: v.CM, entered: make(chan struct{}), release: make(chan struct{})}
	vaultID := glid.New()
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, stuck, v.IM, v.QE))

	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{
		Stats:      &orchStatsAdapter{orch: orch},
		NodeID:     nodeID,
		NodeNameFn: func() string { return nodeID },
		StartTime:  time.Now(),
	})
	collected := time.Now()
	collector.CollectLocalTick(collected)

	srv := server.New(orch, sysmem.NewStore(), orchestrator.Factories{}, nil, server.Config{
		NoAuth:         true,
		NodeID:         nodeID,
		Cluster:        oneNodeCluster{id: nodeID},
		LocalStats:     collector.CollectLocalSnapshot,
		LastLocalStats: collector.LastLocal,
	})
	api := httptest.NewServer(srv.Handler())
	t.Cleanup(api.Close)

	stuck.armed.Store(true)
	removed := make(chan error, 1)
	go func() { removed <- orch.ForceRemoveVault(vaultID) }()
	<-stuck.entered
	t.Cleanup(func() {
		close(stuck.release)
		<-removed
		orch.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), statusAnswerBound)
	defer cancel()
	client := gastrologv1connect.NewLifecycleServiceClient(api.Client(), api.URL)
	resp, err := client.GetClusterStatus(ctx, connect.NewRequest(&apiv1.GetClusterStatusRequest{}))
	if err != nil {
		t.Fatalf("cluster status while a vault teardown holds the orchestrator lock: %v", err)
	}
	if len(resp.Msg.Nodes) != 1 {
		t.Fatalf("status lists %d nodes, want the one", len(resp.Msg.Nodes))
	}
	local := resp.Msg.Nodes[0]
	if local.Stats == nil || local.StatsReceivedAt == nil || !local.StatsReceivedAt.AsTime().Equal(collected) {
		t.Fatalf("local node reports stats %v dated %v, want the last collection's, dated %v",
			local.Stats != nil, local.StatsReceivedAt, collected)
	}
}
