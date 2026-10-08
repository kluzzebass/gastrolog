package app

// A stale NodeConfig entry with no matching Raft voter is exactly the drift
// an operator runs remove-node to clean up — NodeConfig and Raft membership
// can disagree after out-of-order applies or snapshot-install gaps. The
// removal must refuse truthfully (the voter set did not change) AND still
// delete the stale entry, or the drift becomes uncleanable: every attempt
// errors and every attempt leaves the entry behind.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/system/raftfsm"

	hraft "github.com/hashicorp/raft"
)

// startSingleNodeClusterServer boots a bootstrapped single-voter cluster
// server, the minimum real Raft that lets RemoveServer consult a live
// configuration.
func startSingleNodeClusterServer(t *testing.T, nodeID string) *cluster.Server {
	t.Helper()
	srv, err := cluster.New(cluster.Config{ClusterAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	transport := srv.Transport()

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 250 * time.Millisecond

	r, err := hraft.NewRaft(conf, raftfsm.New(),
		hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	boot := hraft.Configuration{Servers: []hraft.Server{
		{ID: hraft.ServerID(nodeID), Address: transport.LocalAddr()},
	}}
	if err := r.BootstrapCluster(boot).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}
	srv.SetRaft(r)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = r.Shutdown().Error()
		srv.Stop()
	})
	select {
	case <-r.LeaderCh():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for single-node leadership")
	}
	return srv
}

func TestRemoveNodeHealsStaleNodeConfigAndTellsTheTruth(t *testing.T) {
	t.Parallel()
	srv := startSingleNodeClusterServer(t, "node-live")

	// The drift: a NodeConfig entry whose node is not in the Raft
	// configuration.
	ctx := context.Background()
	staleID := glid.New()
	cfgStore := sysmem.NewStore()
	if err := cfgStore.PutNode(ctx, system.NodeConfig{ID: staleID, Name: "ghost"}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := executeNodeRemoval(ctx, srv, cfgStore, logger, staleID.String(),
		cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator})

	if !errors.Is(err, cluster.ErrNodeNotInCluster) {
		t.Fatalf("removal of a non-member returned %v, want ErrNodeNotInCluster", err)
	}
	if n, err := cfgStore.GetNode(ctx, staleID); err != nil {
		t.Fatalf("GetNode: %v", err)
	} else if n != nil {
		t.Fatal("the stale NodeConfig entry survived the removal — the drift is uncleanable")
	}
}
