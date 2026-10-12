package app

// Every node of a cluster restarting at once — a host reboot, a power loss —
// must find quorum again on its own. The node that serves join tokens opens
// its HTTP listener only after its Raft group has quorum, so a member that
// waited for a token before bringing its Raft transport up would hold that
// quorum hostage.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/system"
)

// bootingNode is one node driven through the startup sequence Run performs
// before it serves: enrolment, the config store, the cluster port, membership
// and config replication.
type bootingNode struct {
	hd          home.Dir
	id          string
	cfg         RunConfig
	srv         *cluster.Server
	store       *system.StoreProxy
	setSuffrage func(ctx context.Context, targetNodeID string, voter bool) error
}

func (n *bootingNode) boot(ctx context.Context) error {
	logger := discardLogger()
	srv, ctls, err := setupCluster(ctx, logger, n.cfg, n.hd, n.id)
	if err != nil {
		return err
	}
	raw, err := openConfigStore(n.cfg.ConfigType, raftStoreOpts{
		Home: n.hd, NodeID: n.id, JoinAddr: n.cfg.JoinAddr,
		ClusterSrv: srv, ClusterTLS: ctls, Logger: logger,
	})
	if err != nil {
		srv.Stop()
		return fmt.Errorf("open config store: %w", err)
	}
	n.srv, n.store = srv, system.NewStoreProxy(raw)
	if err := startClusterServices(ctx, srv, ctls, n.store, n.hd, n.id, logger); err != nil {
		return err
	}
	appSys, _, err := loadLocalConfig(ctx, logger, n.cfg, n.store, srv, ctls, n.id)
	if err != nil {
		return err
	}
	if err := awaitReplication(ctx, appSys, n.cfg.ConfigType, n.store, logger); err != nil {
		return err
	}
	var gate membershipGate
	n.setSuffrage = makeSetNodeSuffrageFunc(srv, n.store, &gate, n.id, nil, logger)
	return nil
}

// stop shuts the node down in Run's order: the config store's Raft before
// the cluster server whose transport it reads.
func (n *bootingNode) stop() {
	if n.store != nil {
		_ = n.store.Close()
	}
	if n.srv != nil {
		n.srv.Stop()
	}
	n.store, n.srv = nil, nil
}

// reserveClusterAddrs returns loopback addresses free at the time of asking.
// A node keeps its address across a restart, as the Raft configuration
// records it.
func reserveClusterAddrs(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		_ = ln.Close()
	}
	return addrs
}

func requireAllVoters(t *testing.T, observer *bootingNode, nodes []*bootingNode) {
	t.Helper()
	servers, err := observer.srv.Servers()
	if err != nil {
		t.Fatalf("read configuration: %v", err)
	}
	suffrage := make(map[string]string, len(servers))
	for _, s := range servers {
		suffrage[s.ID] = s.Suffrage
	}
	if len(suffrage) != len(nodes) {
		t.Fatalf("configuration has %d servers, want %d: %v", len(suffrage), len(nodes), suffrage)
	}
	for _, n := range nodes {
		if suffrage[n.id] != suffrageVoter {
			t.Fatalf("node %s is %q in the configuration, want Voter", n.id, suffrage[n.id])
		}
	}
}

func TestWholeClusterRestartReachesQuorumWithoutTheTokenSource(t *testing.T) {
	if testing.Short() {
		t.Skip("four-node real-raft cluster formation and simultaneous restart")
	}
	addrs := reserveClusterAddrs(t, 4)

	// The token source is the bootstrap node's HTTP listener: it answers only
	// while that node is up and has quorum.
	var serving atomic.Pointer[system.StoreProxy]
	source := newTokenSource(t, func() (string, error) {
		store := serving.Load()
		if store == nil {
			return "", errors.New("the token-serving node has no quorum yet")
		}
		return makeBootstrapTokenFn(store)()
	})

	nodes := make([]*bootingNode, len(addrs))
	for i, addr := range addrs {
		hd := home.New(t.TempDir())
		cfg := RunConfig{ConfigType: "raft", ClusterAddr: addr}
		if i > 0 {
			cfg = joinerConfig(addrs[0], source.url())
			cfg.ClusterAddr = addr
		}
		nodes[i] = &bootingNode{hd: hd, id: identityOf(t, hd), cfg: cfg}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	bootstrap := nodes[0]
	t.Cleanup(bootstrap.stop)
	if err := bootstrap.boot(ctx); err != nil {
		t.Fatalf("bootstrap node: %v", err)
	}
	serving.Store(bootstrap.store)
	for _, n := range nodes[1:] {
		t.Cleanup(n.stop)
		if err := n.boot(ctx); err != nil {
			t.Fatalf("first join of %s: %v", n.id, err)
		}
		if err := bootstrap.setSuffrage(ctx, n.id, true); err != nil {
			t.Fatalf("promote %s: %v", n.id, err)
		}
	}
	if got := source.hits.Load(); got != 3 {
		t.Fatalf("three first joins asked the token source %d times, want 3", got)
	}
	requireAllVoters(t, bootstrap, nodes)

	// Every node goes down together and comes back together.
	serving.Store(nil)
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Go(n.stop)
	}
	wg.Wait()
	asked := source.hits.Load()

	restarted := make([]*bootingNode, len(nodes))
	for i, n := range nodes {
		restarted[i] = &bootingNode{hd: n.hd, id: n.id, cfg: n.cfg}
		t.Cleanup(restarted[i].stop)
	}
	restartCtx, restartCancel := context.WithTimeout(ctx, time.Minute)
	defer restartCancel()
	errs := make([]error, len(restarted))
	for i, n := range restarted {
		wg.Go(func() {
			errs[i] = n.boot(restartCtx)
			if i == 0 && errs[i] == nil {
				serving.Store(n.store)
			}
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("node %d did not come back from the whole-cluster restart: %v", i, err)
		}
	}
	if got := source.hits.Load() - asked; got != 0 {
		t.Fatalf("restarting members asked the token source %d times, want 0", got)
	}

	// The cluster serves: every node commits through the restored quorum and
	// sees what the others write.
	for _, n := range restarted {
		requireAllVoters(t, n, restarted)
		if err := n.store.Barrier(restartCtx); err != nil {
			t.Fatalf("node %s cannot commit after the restart: %v", n.id, err)
		}
	}
	written := system.NodeConfig{ID: glid.New(), Name: "written-after-restart"}
	if err := restarted[3].store.PutNode(restartCtx, written); err != nil {
		t.Fatalf("write through a joiner after the restart: %v", err)
	}
	if err := restarted[1].store.Barrier(restartCtx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted[1].store.GetNode(restartCtx, written.ID)
	if err != nil || got == nil || got.Name != written.Name {
		t.Fatalf("another node reads %+v, %v; want the write made after the restart", got, err)
	}
}

// Skipping the token on a certificate in the home grants nothing the
// certificate does not: one the cluster CA did not sign gets no membership,
// whether it names a new node or impersonates a member.
func TestForeignCertificateSkipsTheTokenAndIsRefusedByTheCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("real-raft membership over mTLS")
	}
	t.Parallel()
	clusterCA, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	leader := newAdmissionNode(t, clusterCA, true)
	select {
	case <-leader.raft.LeaderCh():
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrap node never became leader")
	}
	before := configurationOf(t, leader)

	for _, tc := range []struct {
		name   string
		nodeID func(hd home.Dir) string
	}{
		{"names a new node", func(hd home.Dir) string { return identityOf(t, hd) }},
		{"impersonates a member", func(home.Dir) string { return leader.id }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foreignCA, err := tlsutil.GenerateCA()
			if err != nil {
				t.Fatal(err)
			}
			hd := home.New(t.TempDir())
			nodeID := tc.nodeID(hd)
			cert, err := tlsutil.GenerateNodeCert(foreignCA.CertPEM, foreignCA.KeyPEM, nodeID, cluster.LaneSANs)
			if err != nil {
				t.Fatal(err)
			}
			if err := hd.EnsureExists(); err != nil {
				t.Fatal(err)
			}
			if err := cluster.SaveFile(hd.ClusterTLSPath(), cert.CertPEM, cert.KeyPEM, foreignCA.CertPEM); err != nil {
				t.Fatal(err)
			}

			source := newTokenSource(t, func() (string, error) { return "", errors.New("no token") })
			cfg := joinerConfig(leader.srv.Addr(), source.url())
			cfg.ClusterAddr = reserveClusterAddrs(t, 1)[0]

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srv, ctls, err := setupCluster(ctx, discardLogger(), cfg, hd, nodeID)
			if err != nil {
				t.Fatalf("setupCluster: %v", err)
			}
			t.Cleanup(srv.Stop)
			if got := source.hits.Load(); got != 0 {
				t.Fatalf("a node holding a certificate asked the token source %d times, want 0", got)
			}

			if err := requestClusterMembership(ctx, discardLogger(), cfg, srv, ctls, nodeID); err == nil {
				t.Fatal("the cluster granted membership to a certificate its CA did not sign")
			}
			if after := configurationOf(t, leader); !sameConfiguration(before, after) {
				t.Fatalf("the configuration moved:\n before %v\n  after %v", before, after)
			}
		})
	}
}
