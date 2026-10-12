package app

// A member that learns of its removal while it is still starting stops
// everything it had started before it serves the removal: a removed node
// runs no orchestrator, no Raft group and no cluster port. The removal is
// delivered through the startup seam at each stage in turn, so the early
// return of every stage is driven, not just the end of a full start.

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/raftgroup"
)

// startupAtRemoval is what had started when the removal was delivered.
type startupAtRemoval struct {
	mu          sync.Mutex
	delivered   bool
	orch        *orchestrator.Orchestrator
	orchRunning bool
	groupMgr    *raftgroup.GroupManager
	groups      int
}

func TestMembershipEndDuringStartupStopsWhatStarted(t *testing.T) {
	if testing.Short() {
		t.Skip("full server starts on a real home, once per startup stage")
	}
	addrs := reserveClusterAddrs(t, 2)
	base := newRunningNode(t, "solo", shortHome(t), RunConfig{ServerAddr: addrs[0], ClusterAddr: addrs[1]})
	base.start()
	waitForAnswer(t, base)
	if _, err := base.client().System.PutVault(context.Background(), connect.NewRequest(&apiv1.PutVaultRequest{
		Config: &apiv1.VaultConfig{Id: glid.New().ToProto(), Name: "v", Enabled: true, Type: apiv1.VaultType_VAULT_TYPE_MEMORY, ReplicationFactor: 1},
	})); err != nil {
		t.Fatalf("create vault: %v", err)
	}
	nodeID := string(base.requireStatus(t).LocalNodeId)
	base.stop()

	t.Cleanup(func() { memberStartupHook = nil })
	for _, tc := range []struct {
		stage       string
		orchStarted bool
	}{
		{"cluster services started", false},
		{"orchestrator started", true},
		{"ready to serve", true},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			at := &startupAtRemoval{}
			memberStartupHook = func(stage, id string, end *membershipEnd, td *memberTeardown) {
				if id != nodeID || stage != tc.stage {
					return
				}
				at.mu.Lock()
				at.delivered = true
				at.orch, at.groupMgr = td.orch, td.groupMgr
				if td.orch != nil {
					at.orchRunning = td.orch.IsRunning()
				}
				if td.groupMgr != nil {
					at.groups = len(td.groupMgr.Groups())
				}
				at.mu.Unlock()
				end.removed("delivered at " + stage)
			}

			n := newRunningNode(t, "solo", copyHome(t, base.hd), base.cfg)
			n.cfg.HomeFlag = n.hd.Root()
			n.start()
			t.Cleanup(n.stop)
			waitFor(t, 30*time.Second, "the node to serve its removal", func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				st, err := n.clusterStatus(ctx)
				return err == nil && st.Removal != nil
			})
			st := n.requireStatus(t)
			if st.Removal.Reason != "delivered at "+tc.stage {
				t.Fatalf("removal %q, want the one delivered at %q", st.Removal.Reason, tc.stage)
			}
			if err, exited := n.exited(); exited {
				t.Fatalf("Run exited instead of serving the removal: %v", err)
			}

			at.mu.Lock()
			defer at.mu.Unlock()
			if !at.delivered {
				t.Fatalf("startup never reached %q", tc.stage)
			}
			if tc.orchStarted {
				if !at.orchRunning || at.groups == 0 {
					t.Fatalf("at %q the orchestrator ran=%v with %d Raft groups; the stage should find both started",
						tc.stage, at.orchRunning, at.groups)
				}
				if at.orch.IsRunning() {
					t.Fatal("the orchestrator still runs while the node serves its removal")
				}
				if groups := at.groupMgr.Groups(); len(groups) != 0 {
					t.Fatalf("Raft groups %v still run while the node serves its removal", groups)
				}
			}
			if conn, err := net.DialTimeout("tcp", n.cfg.ClusterAddr, time.Second); err == nil {
				_ = conn.Close()
				t.Fatal("the cluster port still listens while the node serves its removal")
			}
			n.stop()
		})
	}
}
