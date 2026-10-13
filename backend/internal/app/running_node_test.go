package app

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/server"
)

// logBuffer collects one node's log output so a test can assert what the
// node said.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// lines returns the log lines holding every one of parts.
func (b *logBuffer) lines(parts ...string) []string {
	var out []string
	for line := range strings.SplitSeq(b.String(), "\n") {
		if line == "" {
			continue
		}
		matched := true
		for _, p := range parts {
			if !strings.Contains(line, p) {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, line)
		}
	}
	return out
}

// runningNode is one node running Run in-process, the way the server
// command runs it, with its own home, addresses and log.
type runningNode struct {
	t    *testing.T
	name string
	hd   home.Dir
	cfg  RunConfig
	logs *logBuffer

	cancel context.CancelFunc
	done   chan error
}

// shortHome returns an empty home directory whose socket path fits the
// platform's limit on Unix socket path length.
func shortHome(t *testing.T) home.Dir {
	t.Helper()
	dir, err := os.MkdirTemp("", "gl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return home.New(filepath.Join(dir, "h"))
}

func newRunningNode(t *testing.T, name string, hd home.Dir, cfg RunConfig) *runningNode {
	t.Helper()
	cfg.HomeFlag = hd.Root()
	cfg.ConfigType = "raft"
	cfg.NoAuth = true
	cfg.NodeName = name
	return &runningNode{t: t, name: name, hd: hd, cfg: cfg}
}

// start runs the node with a fresh log, as a process start would.
func (n *runningNode) start() {
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.done = make(chan error, 1)
	n.logs = &logBuffer{}
	logger := slog.New(slog.NewTextHandler(n.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	go func() { n.done <- Run(ctx, logger, n.cfg) }()
}

// stop cancels the node and waits for Run to return.
func (n *runningNode) stop() {
	if n.cancel == nil {
		return
	}
	n.cancel()
	select {
	case <-n.done:
	case <-time.After(time.Minute):
		n.t.Errorf("node %s did not shut down within a minute", n.name)
	}
	n.cancel = nil
}

// exited reports Run's return value if Run returned on its own.
func (n *runningNode) exited() (error, bool) {
	select {
	case err := <-n.done:
		n.done <- err
		return err, true
	default:
		return nil, false
	}
}

// client talks to the node over its home's Unix socket, as the CLI does
// when no address is given.
func (n *runningNode) client() *server.Client {
	sock := n.hd.SocketPath()
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}}
	return server.NewClientWithHTTP(hc, "http://localhost")
}

func (n *runningNode) clusterStatus(ctx context.Context) (*apiv1.GetClusterStatusResponse, error) {
	resp, err := n.client().Lifecycle.GetClusterStatus(ctx, connect.NewRequest(&apiv1.GetClusterStatusRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// statusAnswerBound is how long a status call may take before the test
// calls it hung. A status call reads in-memory state; a node that needs
// longer than this is waiting on something it should not be.
const statusAnswerBound = 5 * time.Second

// requireStatus asks the node for cluster status and fails the test unless
// it answers within statusAnswerBound.
func (n *runningNode) requireStatus(t *testing.T) *apiv1.GetClusterStatusResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), statusAnswerBound)
	defer cancel()
	st, err := n.clusterStatus(ctx)
	if err != nil {
		t.Fatalf("cluster status on %s did not answer within %s: %v", n.name, statusAnswerBound, err)
	}
	return st
}

// configuration is the Raft configuration the node reports, node ID to
// suffrage.
func configuration(st *apiv1.GetClusterStatusResponse) map[string]apiv1.ClusterNodeSuffrage {
	out := make(map[string]apiv1.ClusterNodeSuffrage, len(st.Nodes))
	for _, nd := range st.Nodes {
		out[string(nd.Id)] = nd.Suffrage
	}
	return out
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", within, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// joinerRunConfig is a joining node's configuration as the Kubernetes
// manifest gives it: a join address and a token URL on every start.
func joinerRunConfig(t *testing.T, boot *runningNode) RunConfig {
	return RunConfig{
		ServerAddr:           reserveClusterAddrs(t, 1)[0],
		ClusterAddr:          reserveClusterAddrs(t, 1)[0],
		JoinAddr:             boot.cfg.ClusterAddr,
		BootstrapTokenURL:    "http://" + boot.cfg.ServerAddr + "/cluster/bootstrap-token",
		BootstrapTokenSecret: tokenSourceSecret,
	}
}

// formCluster starts n nodes the way the Kubernetes manifest does — the
// first bootstraps and serves join tokens, the rest join with a token URL —
// and returns once every node is a voter. Every node then holds a replica
// of each of the given number of memory vaults.
func formCluster(t *testing.T, n, vaults int) []*runningNode {
	t.Helper()
	addrs := reserveClusterAddrs(t, 2)
	boot := newRunningNode(t, "node-0", shortHome(t), RunConfig{
		ServerAddr:                addrs[0],
		ClusterAddr:               addrs[1],
		BootstrapTokenServeSecret: tokenSourceSecret,
	})
	nodes := []*runningNode{boot}
	boot.start()
	t.Cleanup(boot.stop)
	waitFor(t, time.Minute, "the bootstrap node to answer", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := boot.clusterStatus(ctx)
		return err == nil
	})
	for i := 1; i < n; i++ {
		nd := newRunningNode(t, fmt.Sprintf("node-%d", i), shortHome(t), joinerRunConfig(t, boot))
		nodes = append(nodes, nd)
		nd.start()
		t.Cleanup(nd.stop)
	}
	waitFor(t, 2*time.Minute, "every node to be a voter", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		st, err := boot.clusterStatus(ctx)
		if err != nil || len(st.Nodes) != n {
			return false
		}
		for _, nd := range st.Nodes {
			if nd.Suffrage != apiv1.ClusterNodeSuffrage_CLUSTER_NODE_SUFFRAGE_VOTER {
				return false
			}
		}
		return true
	})
	for i := range vaults {
		_, err := boot.client().System.PutVault(context.Background(), connect.NewRequest(&apiv1.PutVaultRequest{
			Config: &apiv1.VaultConfig{
				Id:                glid.New().ToProto(),
				Name:              fmt.Sprintf("vault-%d", i),
				Enabled:           true,
				Type:              apiv1.VaultType_VAULT_TYPE_MEMORY,
				ReplicationFactor: uint32(n), //nolint:gosec // a handful of test nodes
			},
		}))
		if err != nil {
			t.Fatalf("create vault: %v", err)
		}
	}
	return nodes
}
