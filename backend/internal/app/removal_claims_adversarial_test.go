package app

// Word of a removal ends a membership for good, so who may give it is a
// security boundary. A removed node still holds a certificate the cluster CA
// signed and still answers whatever dials it — the leader's eviction notice
// first of all — and a node at the join address is only known to hold such a
// certificate. Neither may make a member declare itself removed.

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/home"
)

// forgingNode answers every cluster call at addr with a certificate the
// cluster CA signed, refusing the caller as removed: from the first call on,
// or only once it has been sent an eviction notice, as a removed node would.
type forgingNode struct {
	evicted     atomic.Bool
	forgeAlways bool

	mu      sync.Mutex
	forged  map[string]int // method → refusals forged
	callers map[string]int // caller → refusals forged
	stop    func()
}

func startForgingNode(t *testing.T, addr string, cert tls.Certificate, forgeAlways bool) *forgingNode {
	t.Helper()
	f := &forgingNode{forgeAlways: forgeAlways, forged: map[string]int{}, callers: map[string]int{}}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequestClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	srv := grpc.NewServer(grpc.Creds(creds), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		caller := ""
		if p, ok := peer.FromContext(stream.Context()); ok {
			if info, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(info.State.PeerCertificates) > 0 {
				caller = tlsutil.NodeIDFromCert(info.State.PeerCertificates[0])
			}
		}
		if strings.HasSuffix(method, "/NotifyEviction") {
			f.evicted.Store(true)
		}
		if !f.forgeAlways && !f.evicted.Load() {
			return status.Error(codes.Unavailable, "not answering yet")
		}
		f.mu.Lock()
		f.forged[method]++
		f.callers[caller]++
		f.mu.Unlock()
		st, err := status.New(codes.PermissionDenied, "node "+caller+" was removed from this cluster").
			WithDetails(&apiv1.RemovedNodeRefusal{NodeId: caller})
		if err != nil {
			return err
		}
		return st.Err()
	}))
	go func() { _ = srv.Serve(ln) }()
	f.stop = srv.Stop
	t.Cleanup(srv.Stop)
	return f
}

// forgedTo is how many refusals the forger has sent caller.
func (f *forgingNode) forgedTo(caller string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callers[caller]
}

func (f *forgingNode) forgedFor(methodSuffix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for m, c := range f.forged {
		if strings.HasSuffix(m, methodSuffix) {
			n += c
		}
	}
	return n
}

func certificateOf(t *testing.T, hd home.Dir) tls.Certificate {
	t.Helper()
	return loadCert(t, hd).State().Cert
}

// The leader removes a node whose machine is in an attacker's hands. The
// attacker answers the eviction notice — and every call after it — refusing
// the caller as removed, naming it from its own client certificate. No
// member, the leader least of all, may take that as word of its removal.
func TestARemovedNodeCannotTakeOutTheNodesThatRemovedIt(t *testing.T) {
	if testing.Short() {
		t.Skip("four in-process nodes running the full server")
	}
	nodes := formCluster(t, 4, removedTestVaults)
	boot := nodes[0]
	st := boot.requireStatus(t)
	leaderID := string(st.LeaderId)
	var leader, victim *runningNode
	for _, n := range nodes {
		id := string(n.requireStatus(t).LocalNodeId)
		switch {
		case id == leaderID:
			leader = n
		case victim == nil:
			victim = n
		}
	}
	if leader == nil || victim == nil {
		t.Fatalf("no leader (%s) or victim among the nodes", leaderID)
	}
	victimID := string(victim.requireStatus(t).LocalNodeId)
	victim.stop()
	forger := startForgingNode(t, victim.cfg.ClusterAddr, certificateOf(t, victim.hd), false)

	if _, err := leader.client().Lifecycle.RemoveNode(context.Background(), connect.NewRequest(&apiv1.RemoveNodeRequest{
		NodeId: []byte(victimID), Force: true,
	})); err != nil {
		t.Fatalf("remove %s: %v", victimID, err)
	}
	waitFor(t, time.Minute, "the leader's eviction notice to reach the removed node", func() bool {
		return forger.forgedFor("/NotifyEviction") > 0
	})

	for _, n := range nodes {
		if n == victim {
			continue
		}
		st := n.requireStatus(t)
		if st.Removal != nil {
			t.Fatalf("%s took the removed node's word and declared itself removed: %+v", n.name, st.Removal)
		}
		if cfg := configuration(st); len(cfg) != 3 {
			t.Fatalf("%s reports configuration %v, want the three remaining members", n.name, cfg)
		}
		if lines := n.logs.lines("level=ERROR", "was removed from the cluster"); len(lines) != 0 {
			t.Fatalf("%s logged its own removal: %v", n.name, lines)
		}
	}
	// The cluster still commits: a removal claim that had taken the leader
	// out would have cost the remaining three their quorum.
	if _, err := boot.client().System.PutVault(context.Background(), connect.NewRequest(&apiv1.PutVaultRequest{
		Config: &apiv1.VaultConfig{Name: "after-the-forgery", Enabled: true, Type: apiv1.VaultType_VAULT_TYPE_MEMORY, ReplicationFactor: 1},
	})); err != nil {
		t.Fatalf("the cluster no longer commits after the forged refusals: %v", err)
	}
}

// A node at the join address is only known to hold a certificate the
// cluster CA signed. Its claim that a fresh home was removed — a fresh node
// ID is in no removal record — is not recorded: startup stops with the
// reason, and the next start asks again.
func TestAForgedJoinRefusalIsNeitherTrustedNorRecorded(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	forgerCert, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, "node-forger", cluster.LaneSANs)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(forgerCert.CertPEM, forgerCert.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	addrs := reserveClusterAddrs(t, 2)
	forger := startForgingNode(t, addrs[0], pair, true)

	hd := shortHome(t)
	nodeID := identityOf(t, hd)
	own, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, nodeID, cluster.LaneSANs)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.SaveFile(hd.ClusterTLSPath(), own.CertPEM, own.KeyPEM, ca.CertPEM); err != nil {
		t.Fatal(err)
	}
	cfg := RunConfig{HomeFlag: hd.Root(), ConfigType: "raft", NoAuth: true, ClusterAddr: addrs[1], JoinAddr: addrs[0]}

	for attempt := 1; attempt <= 2; attempt++ {
		logs := &logBuffer{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err := Run(ctx, slog.New(slog.NewTextHandler(logs, nil)), cfg)
		cancel()
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("start %d: Run returned %v; want startup to stop on the refusal", attempt, err)
		}
		if lines := logs.lines("level=ERROR", "refused this node's membership as removed", "nothing is recorded"); len(lines) != 1 {
			t.Fatalf("start %d: want one line saying the claim was not recorded:\n%s", attempt, logs)
		}
		ident, rerr := resolveNodeID(hd, discardLogger())
		if rerr != nil {
			t.Fatal(rerr)
		}
		if ident.ID.String() != nodeID || ident.Removal != nil {
			t.Fatalf("start %d: home now %+v; want %s with no removal recorded", attempt, ident, nodeID)
		}
		if got := forger.forgedFor("/RequestMembership"); got != attempt {
			t.Fatalf("start %d: membership asked %d times, want once per start", attempt, got)
		}
	}
}

// No single member's word ends another's membership. A compromised member
// that refuses every node dialling it as removed takes nobody out; a member
// cut off from the rest, whose only reachable peer is the node the cluster
// just removed, is not taken out by that node; and a restarting member with
// a stale configuration does not record a removal on one join answer.
func TestNoSingleMemberCanEndAnothersMembership(t *testing.T) {
	if testing.Short() {
		t.Skip("five in-process nodes running the full server")
	}
	nodes := formCluster(t, 5, 0)
	ids := make(map[*runningNode]string, len(nodes))
	for _, n := range nodes {
		ids[n] = string(n.requireStatus(t).LocalNodeId)
	}
	compromised, cutOff := nodes[4], nodes[3]
	genuine := nodes[:3]
	compromisedCert := certificateOf(t, compromised.hd)

	t.Run("a compromised member refusing every node takes nobody out", func(t *testing.T) {
		compromised.stop()
		forger := startForgingNode(t, compromised.cfg.ClusterAddr, compromisedCert, true)
		waitFor(t, time.Minute, "the forger to refuse every other node", func() bool {
			for _, n := range nodes[:4] {
				if forger.forgedTo(ids[n]) == 0 {
					return false
				}
			}
			return true
		})
		waiting := 0
		for _, n := range nodes[:4] {
			st := n.requireStatus(t)
			if st.Removal != nil {
				t.Fatalf("%s took one member's word and declared itself removed: %+v", n.name, st.Removal)
			}
			if len(st.Nodes) != len(nodes) {
				t.Fatalf("%s reports configuration %v, want all five", n.name, configuration(st))
			}
			waiting += len(n.logs.lines("a member refuses this node as removed", "claimant_node_id="+ids[compromised]))
		}
		if waiting == 0 {
			t.Fatal("no node weighed the forged refusals; the test did not reach the majority rule")
		}
		forger.stop()
	})

	t.Run("a member cut off with only the just-removed node reachable is not taken out", func(t *testing.T) {
		compromised.stop()
		cutOff.stop()
		var leader *runningNode
		for _, n := range genuine {
			if st := n.requireStatus(t); string(st.LeaderId) == ids[n] {
				leader = n
			}
		}
		if leader == nil {
			t.Fatal("no leader among the three running members")
		}
		if _, err := leader.client().Lifecycle.RemoveNode(context.Background(), connect.NewRequest(&apiv1.RemoveNodeRequest{
			NodeId: []byte(ids[compromised]), Force: true,
		})); err != nil {
			t.Fatalf("remove %s: %v", ids[compromised], err)
		}
		forger := startForgingNode(t, compromised.cfg.ClusterAddr, compromisedCert, true)
		for _, n := range genuine {
			n.stop()
		}

		// Cut off, the member cannot reach quorum, so it never finishes
		// starting; what is checked is that it does not take the removed
		// node's word while it waits.
		cutOff.start()
		waitFor(t, time.Minute, "the removed node to refuse the cut-off member several times", func() bool {
			return forger.forgedTo(ids[cutOff]) >= 3 || len(cutOff.logs.lines("was removed from the cluster")) > 0
		})
		if lines := cutOff.logs.lines("was removed from the cluster"); len(lines) != 0 {
			t.Fatalf("the cut-off member took the removed node's word: %v", lines)
		}
		if lines := cutOff.logs.lines("a member refuses this node as removed", "claimant_node_id="+ids[compromised]); len(lines) == 0 {
			t.Fatal("the cut-off member never weighed the refusals; the test did not reach the majority rule")
		}
		if err, exited := cutOff.exited(); exited {
			t.Fatalf("the cut-off member's Run returned: %v", err)
		}
		cutOff.stop()
		ident, err := resolveNodeID(cutOff.hd, discardLogger())
		if err != nil {
			t.Fatal(err)
		}
		if ident.Removal != nil {
			t.Fatalf("the cut-off member recorded a removal on the removed node's word: %+v", ident.Removal)
		}

		t.Run("one join answer to a restarting member records nothing", func(t *testing.T) {
			hd := copyHome(t, cutOff.hd)
			cfg := cutOff.cfg
			cfg.HomeFlag = hd.Root()
			cfg.ServerAddr = ""
			cfg.ClusterAddr = reserveClusterAddrs(t, 1)[0]
			cfg.JoinAddr = compromised.cfg.ClusterAddr
			asked := forger.forgedFor("/RequestMembership")
			logs := &logBuffer{}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			err := Run(ctx, slog.New(slog.NewTextHandler(logs, nil)), cfg)
			cancel()
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run returned %v; want the start to stop on the refusal", err)
			}
			if got := forger.forgedFor("/RequestMembership"); got != asked+1 {
				t.Fatalf("membership asked %d times, want once", got-asked)
			}
			if lines := logs.lines("level=ERROR", "refused this node's membership as removed", "nothing is recorded"); len(lines) != 1 {
				t.Fatalf("want one line saying the claim was not recorded:\n%s", logs)
			}
			ident, rerr := resolveNodeID(hd, discardLogger())
			if rerr != nil {
				t.Fatal(rerr)
			}
			if ident.Removal != nil {
				t.Fatalf("one join answer recorded a removal: %+v", ident.Removal)
			}
		})
	})
}
