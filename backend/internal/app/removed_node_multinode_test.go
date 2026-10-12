package app

// A node the operator removes stays removed. It does not rejoin on its own,
// and it does not start over as a cluster of its own under a new identity
// while holding its old certificate: it serves the account of its removal,
// from this process and from every later start of the same home, until the
// operator brings the machine back from an empty home as a new node. The
// cluster, for its part, refuses every way back in the removed identity
// offers and says so in its log, naming the identity presented.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/server"
)

const removedTestVaults = 2

func TestRemovedNodeStaysRemoved(t *testing.T) {
	if testing.Short() {
		t.Skip("four in-process nodes running the full server, removal and restarts")
	}
	nodes := formCluster(t, 4, removedTestVaults)
	boot, victim := nodes[0], nodes[3]
	members := nodes[:3]

	victimID := string(victim.requireStatus(t).LocalNodeId)
	waitFor(t, time.Minute, "the victim to host every vault", func() bool {
		for _, nd := range victim.requireStatus(t).Nodes {
			if string(nd.Id) == victimID && nd.Stats != nil && len(nd.Stats.Vaults) == removedTestVaults {
				return true
			}
		}
		return false
	})
	certBefore := readFile(t, victim.hd.ClusterTLSPath())

	ctx := context.Background()
	if _, err := boot.client().Lifecycle.RemoveNode(ctx, connect.NewRequest(&apiv1.RemoveNodeRequest{
		NodeId: []byte(victimID), Force: true,
	})); err != nil {
		t.Fatalf("remove %s: %v", victimID, err)
	}

	t.Run("the running node serves its removal", func(t *testing.T) {
		var st *apiv1.GetClusterStatusResponse
		waitFor(t, time.Minute, "the victim to report its removal", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := victim.clusterStatus(ctx)
			return err == nil && got.Removal != nil
		})
		st = victim.requireStatus(t)
		requireReportsRemoval(t, victim, st, victimID)
		if st.Removal.Reason != "removed from cluster by leader" {
			t.Fatalf("removal reason %q, want the leader's eviction notice", st.Removal.Reason)
		}
		requireRemovalLogged(t, victim, victimID)
		requireStaysUp(t, victim)
	})

	removedAt := victim.requireStatus(t).Removal.RemovedAt.AsTime()

	t.Run("members answer status and no longer list it", func(t *testing.T) {
		for _, m := range members {
			cfg := configuration(m.requireStatus(t))
			if _, listed := cfg[victimID]; listed || len(cfg) != len(members) {
				t.Fatalf("%s reports configuration %v, want the three members without %s", m.name, cfg, victimID)
			}
		}
	})

	t.Run("restarts of its old home stay removed and quiet", func(t *testing.T) {
		for range 3 {
			victim.stop()
			victim.start()
			waitForAnswer(t, victim)
			st := victim.requireStatus(t)
			requireReportsRemoval(t, victim, st, victimID)
			if got := st.Removal.RemovedAt.AsTime(); !got.Equal(removedAt) {
				t.Fatalf("restart reports removal at %v, want the original %v", got, removedAt)
			}
			requireRemovalLogged(t, victim, victimID)
			if errs := victim.logs.lines("level=ERROR"); len(errs) != 1 {
				t.Fatalf("a restart logged %d errors, want only its removal:\n%s", len(errs), strings.Join(errs, "\n"))
			}
			if lines := victim.logs.lines("generated new node_id"); len(lines) != 0 {
				t.Fatalf("restart minted an identity: %v", lines)
			}
			requireStaysUp(t, victim)
		}
		if !bytes.Equal(readFile(t, victim.hd.ClusterTLSPath()), certBefore) {
			t.Fatal("the restarts changed the node's certificate")
		}
		if backups, _ := filepath.Glob(victim.hd.RaftDir() + ".bak.*"); len(backups) != 0 {
			t.Fatalf("the removal moved the raft directory aside: %v", backups)
		}
		for _, m := range members {
			if _, listed := configuration(m.requireStatus(t))[victimID]; listed {
				t.Fatalf("%s lists %s again after its restarts", m.name, victimID)
			}
		}
	})

	t.Run("its old identity cannot get back in", func(t *testing.T) {
		ctls := loadCert(t, victim.hd)
		before := configuration(boot.requireStatus(t))
		entry := members[1]

		err := cluster.JoinCluster(boundedCtx(t), nil, entry.cfg.ClusterAddr, victimID, victim.cfg.ClusterAddr, ctls)
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("membership for the removed identity: %v, want PermissionDenied", err)
		}
		if lines := entry.logs.lines("membership refused: the cluster removed this node",
			"requested_node_id="+victimID, "certificate_node_id="+victimID); len(lines) == 0 {
			t.Fatalf("%s did not log refusing %s with the identity presented:\n%s", entry.name, victimID, entry.logs)
		}

		token := fetchJoinToken(t, boot)
		secret, caHash, err := tlsutil.ParseJoinToken(token)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.Enroll(boundedCtx(t), entry.cfg.ClusterAddr, secret, caHash, victimID, victim.cfg.ClusterAddr); err == nil {
			t.Fatal("a member issued a certificate naming the removed identity")
		}

		requireConfigurationUnchanged(t, boot, before)
	})

	t.Run("its old certificate cannot bring in a new identity", func(t *testing.T) {
		ctls := loadCert(t, victim.hd)
		before := configuration(boot.requireStatus(t))
		entry := members[2]
		newID := glid.New().String()

		err := cluster.JoinCluster(boundedCtx(t), nil, entry.cfg.ClusterAddr, newID, victim.cfg.ClusterAddr, ctls)
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("membership for a new identity under the removed certificate: %v, want PermissionDenied", err)
		}
		if lines := entry.logs.lines("membership refused: the certificate names another node",
			"requested_node_id="+newID, "certificate_node_id="+victimID); len(lines) == 0 {
			t.Fatalf("%s did not log refusing the certificate with both identities:\n%s", entry.name, entry.logs)
		}
		requireConfigurationUnchanged(t, boot, before)
	})

	t.Run("its old certificate gets no other cluster call", func(t *testing.T) {
		ctls := loadCert(t, victim.hd)
		target := members[2]
		conn, err := grpc.NewClient(target.cfg.ClusterAddr, grpc.WithTransportCredentials(ctls.TransportCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()

		// Telling a member it was evicted is the most a removed certificate
		// could do with a member's trust.
		for range 3 {
			err = cluster.NewNotifyEvictionClient(conn).NotifyEviction(boundedCtx(t), "forged by a removed node")
			if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "was removed from this cluster") {
				t.Fatalf("a call under the removed certificate: %v, want PermissionDenied naming the removal", err)
			}
		}
		if lines := target.logs.lines("refusing cluster calls from a node the cluster removed", "certificate_node_id="+victimID); len(lines) != 1 {
			t.Fatalf("%s logged refusing %s %d times for three calls, want once:\n%s", target.name, victimID, len(lines), target.logs)
		}
		if st := target.requireStatus(t); st.Removal != nil || len(st.Nodes) != len(members) {
			t.Fatalf("%s acted on the forged eviction: removal %v, configuration %v", target.name, st.Removal, configuration(st))
		}
	})

	t.Run("a member that does not lead cannot evict another", func(t *testing.T) {
		leaderID := string(boot.requireStatus(t).LeaderId)
		var sender, target *runningNode
		for _, m := range members {
			id := string(m.requireStatus(t).LocalNodeId)
			switch {
			case sender == nil && id != leaderID:
				sender = m
			case target == nil:
				target = m
			}
		}
		conn, err := grpc.NewClient(target.cfg.ClusterAddr, grpc.WithTransportCredentials(loadCert(t, sender.hd).TransportCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		err = cluster.NewNotifyEvictionClient(conn).NotifyEviction(boundedCtx(t), "forged by a member that does not lead")
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("eviction notice from %s, which does not lead: %v, want PermissionDenied", sender.name, err)
		}
		if lines := target.logs.lines("eviction notice refused", "caller_node_id="+string(sender.requireStatus(t).LocalNodeId)); len(lines) == 0 {
			t.Fatalf("%s did not log refusing the eviction notice:\n%s", target.name, target.logs)
		}
		if st := target.requireStatus(t); st.Removal != nil || len(st.Nodes) != len(members) {
			t.Fatalf("%s acted on an eviction notice from a member that does not lead: removal %v, configuration %v",
				target.name, st.Removal, configuration(st))
		}
	})

	t.Run("a home missing its identity but holding the certificate is refused", func(t *testing.T) {
		before := configuration(boot.requireStatus(t))
		for _, tc := range []struct {
			name   string
			remove func(hd home.Dir) string
		}{
			{"raft directory deleted", func(hd home.Dir) string { return hd.RaftDir() }},
			{"node identity store deleted", func(hd home.Dir) string { return hd.ClusterCtlWALDir() }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				hd := copyHome(t, victim.hd)
				if err := os.RemoveAll(tc.remove(hd)); err != nil {
					t.Fatal(err)
				}
				cfg := joinerRunConfig(t, boot)
				cfg.HomeFlag, cfg.ConfigType, cfg.NoAuth = hd.Root(), "raft", true

				ctx, cancel := context.WithTimeout(context.Background(), statusAnswerBound)
				defer cancel()
				err := Run(ctx, discardLogger(), cfg)
				if !errors.Is(err, errIncompleteHome) {
					t.Fatalf("Run on the home: %v, want it refused as incomplete", err)
				}
				if raw, err := readStableStoreNodeID(t, hd); err != nil || len(raw) != 0 {
					t.Fatalf("the home now holds identity %x (%v), want none minted", raw, err)
				}
				requireConfigurationUnchanged(t, boot, before)
			})
		}
	})

	t.Run("a node down while it is removed comes back removed", func(t *testing.T) {
		downed := members[2]
		downedID := string(downed.requireStatus(t).LocalNodeId)
		downed.stop()
		if _, err := boot.client().Lifecycle.RemoveNode(context.Background(), connect.NewRequest(&apiv1.RemoveNodeRequest{
			NodeId: []byte(downedID), Force: true,
		})); err != nil {
			t.Fatalf("remove the stopped %s: %v", downedID, err)
		}

		downed.start()
		waitFor(t, time.Minute, "the node to learn its removal from the members", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			st, err := downed.clusterStatus(ctx)
			return err == nil && st.Removal != nil
		})
		st := downed.requireStatus(t)
		requireReportsRemoval(t, downed, st, downedID)
		if !strings.Contains(st.Removal.Reason, "refused as removed by cluster member") {
			t.Fatalf("removal reason %q, want the refusing member", st.Removal.Reason)
		}
		requireRemovalLogged(t, downed, downedID)
		requireStaysUp(t, downed)
		refused := 0
		for _, m := range members[:2] {
			refused += len(m.logs.lines("refusing cluster calls from a node the cluster removed", "certificate_node_id="+downedID))
		}
		if refused == 0 {
			t.Fatal("no member logged refusing the removed node's calls")
		}

		downed.stop()
		downed.start()
		waitForAnswer(t, downed)
		st = downed.requireStatus(t)
		requireReportsRemoval(t, downed, st, downedID)
		requireRemovalLogged(t, downed, downedID)
		if errs := downed.logs.lines("level=ERROR"); len(errs) != 1 {
			t.Fatalf("the restart after learning the removal logged %d errors, want only the removal:\n%s", len(errs), strings.Join(errs, "\n"))
		}
		requireStaysUp(t, downed)
		if _, listed := configuration(boot.requireStatus(t))[downedID]; listed {
			t.Fatalf("%s is back in the configuration", downedID)
		}
	})

	t.Run("the machine comes back from an empty home as a new node", func(t *testing.T) {
		before := configuration(boot.requireStatus(t))
		victim.stop()
		reborn := newRunningNode(t, "node-3-reborn", shortHome(t), victim.cfg)
		reborn.cfg.HomeFlag = reborn.hd.Root()
		reborn.start()
		t.Cleanup(reborn.stop)

		var newID string
		waitFor(t, 2*time.Minute, "the new node to be a voter", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			st, err := reborn.clusterStatus(ctx)
			if err != nil || st.Removal != nil {
				return false
			}
			newID = string(st.LocalNodeId)
			return configuration(boot.requireStatus(t))[newID] == apiv1.ClusterNodeSuffrage_CLUSTER_NODE_SUFFRAGE_VOTER
		})
		if newID == victimID {
			t.Fatalf("the machine came back under its removed identity %s", victimID)
		}
		cfg := configuration(boot.requireStatus(t))
		if _, listed := cfg[victimID]; listed || len(cfg) != len(before)+1 {
			t.Fatalf("configuration %v, want %v and %s, without %s", cfg, before, newID, victimID)
		}
	})
}

// requireReportsRemoval asserts status from the removed node is its removal
// and nothing a member reports.
func requireReportsRemoval(t *testing.T, n *runningNode, st *apiv1.GetClusterStatusResponse, id string) {
	t.Helper()
	if st.Removal == nil {
		t.Fatalf("%s reports no removal: %+v", n.name, st)
	}
	if string(st.LocalNodeId) != id {
		t.Fatalf("%s reports itself as %s, want its removed identity %s", n.name, st.LocalNodeId, id)
	}
	if st.Removal.Reason == "" || st.Removal.RemovedAt == nil {
		t.Fatalf("%s reports removal %+v without a reason and time", n.name, st.Removal)
	}
	if len(st.Nodes) != 0 || st.LocalStats != nil || len(st.LeaderId) != 0 {
		t.Fatalf("%s reports cluster state after its removal: %+v", n.name, st)
	}
}

// requireRemovalLogged asserts the node's log states the removal once, with
// the identity and the remedy, and nothing of a node trying to lead.
func requireRemovalLogged(t *testing.T, n *runningNode, id string) {
	t.Helper()
	waitFor(t, statusAnswerBound, "the removal in the log", func() bool {
		return len(n.logs.lines("level=ERROR", "was removed from the cluster", "node_id="+id, "empty home directory")) == 1
	})
	if lines := n.logs.lines("YieldLeadership"); len(lines) != 0 {
		t.Fatalf("%s logged leadership activity: %v", n.name, lines)
	}
}

// requireStaysUp asserts the removed node keeps running, holds no cluster
// port, answers the preStop hook, and reads alive but not ready.
func requireStaysUp(t *testing.T, n *runningNode) {
	t.Helper()
	if err, exited := n.exited(); exited {
		t.Fatalf("%s exited: %v", n.name, err)
	}
	if conn, err := net.DialTimeout("tcp", n.cfg.ClusterAddr, time.Second); err == nil {
		_ = conn.Close()
		t.Fatalf("%s still listens on its cluster port %s", n.name, n.cfg.ClusterAddr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), statusAnswerBound)
	defer cancel()
	resp, err := n.client().Lifecycle.YieldLeadership(ctx, connect.NewRequest(&apiv1.YieldLeadershipRequest{}))
	if err != nil || resp.Msg.Transferred {
		t.Fatalf("yield-leadership on %s: %v, %v; want answered with nothing transferred", n.name, resp, err)
	}
	requirePublicSurfaceOnly(t, n)
}

// requirePublicSurfaceOnly asserts the removed node's TCP listener, which
// authenticates nobody, shows the probes and the bare fact of the removal:
// not the node, not when or why.
func requirePublicSurfaceOnly(t *testing.T, n *runningNode) {
	t.Helper()
	st := n.requireStatus(t)
	secrets := []string{string(st.LocalNodeId), st.Removal.GetReason(), server.RemovedNodeRemedy}
	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		code, body := httpGet(t, "http://"+n.cfg.ServerAddr+path)
		if code != want {
			t.Fatalf("%s %s = %d, want %d", n.name, path, code, want)
		}
		if code == http.StatusServiceUnavailable && strings.TrimSpace(body) != server.RemovedNodePublicNotice {
			t.Fatalf("%s %s over TCP says %q, want only %q", n.name, path, body, server.RemovedNodePublicNotice)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), statusAnswerBound)
	defer cancel()
	_, err := server.NewClient("http://"+n.cfg.ServerAddr).Lifecycle.GetClusterStatus(ctx, connect.NewRequest(&apiv1.GetClusterStatusRequest{}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("cluster status over TCP on %s: %v, want FailedPrecondition", n.name, err)
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatalf("cluster status over TCP on %s reveals %q: %v", n.name, secret, err)
		}
	}
}

// waitForAnswer waits for a starting node to serve its API.
func waitForAnswer(t *testing.T, n *runningNode) {
	t.Helper()
	waitFor(t, statusAnswerBound, n.name+" to answer", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := n.clusterStatus(ctx)
		return err == nil
	})
}

func requireConfigurationUnchanged(t *testing.T, observer *runningNode, before map[string]apiv1.ClusterNodeSuffrage) {
	t.Helper()
	after := configuration(observer.requireStatus(t))
	if len(after) != len(before) {
		t.Fatalf("the configuration moved:\n before %v\n  after %v", before, after)
	}
	for id, s := range before {
		if after[id] != s {
			t.Fatalf("the configuration moved:\n before %v\n  after %v", before, after)
		}
	}
}

func boundedCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), statusAnswerBound)
	t.Cleanup(cancel)
	return ctx
}

func loadCert(t *testing.T, hd home.Dir) *cluster.ClusterTLS {
	t.Helper()
	ctls := cluster.NewClusterTLS()
	if found, err := ctls.LoadFile(hd.ClusterTLSPath()); err != nil || !found {
		t.Fatalf("load %s: found=%v err=%v", hd.ClusterTLSPath(), found, err)
	}
	return ctls
}

func fetchJoinToken(t *testing.T, boot *runningNode) string {
	t.Helper()
	req, err := http.NewRequestWithContext(boundedCtx(t), http.MethodGet, "http://"+boot.cfg.ServerAddr+"/cluster/bootstrap-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(bootstrapTokenSecretHeader, tokenSourceSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("join token: %d %s", resp.StatusCode, body)
	}
	return string(body)
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(boundedCtx(t), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // G304: a test home
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// copyHome copies a stopped node's home so a test can damage the copy.
func copyHome(t *testing.T, src home.Dir) home.Dir {
	t.Helper()
	dst := shortHome(t)
	err := filepath.WalkDir(src.Root(), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src.Root(), path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst.Root(), rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		if d.Type()&os.ModeSocket != 0 {
			return nil
		}
		data, err := os.ReadFile(path) //nolint:gosec // G304: a test home
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}
