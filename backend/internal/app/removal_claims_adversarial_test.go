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
	callers map[string]bool
}

func startForgingNode(t *testing.T, addr string, cert tls.Certificate, forgeAlways bool) *forgingNode {
	t.Helper()
	f := &forgingNode{forgeAlways: forgeAlways, forged: map[string]int{}, callers: map[string]bool{}}
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
		f.callers[caller] = true
		f.mu.Unlock()
		st, err := status.New(codes.PermissionDenied, "node "+caller+" was removed from this cluster").
			WithDetails(&apiv1.RemovedNodeRefusal{NodeId: caller})
		if err != nil {
			return err
		}
		return st.Err()
	}))
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return f
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
