package server_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Multi-node coverage for the port check. Listener availability is a per-node
// fact, so the interesting cases all involve a node OTHER than the one serving
// the request — which is exactly what the single-node tests cannot reach.
//
// Each node gets a real SystemServer over a real orchestrator with the real
// syslog registration, so the trial bind is a real bind against a real port.
// The forwarder is a stand-in that dispatches straight to the target node's own
// LocalIngesterCheck, the same shortcut mnPeerStorageStats and mnPeerVaultStats
// take elsewhere in this package: it skips the gRPC hop while keeping the thing
// under test — which node answers — honest.

// mnIngesterCheckers dispatches a per-node validation request to that node's
// real checker.
type mnIngesterCheckers struct {
	byNode map[string]func(context.Context, string, map[string]string, []byte) (bool, string)
}

func (m *mnIngesterCheckers) ValidateIngester(ctx context.Context, nodeID string, req *apiv1.ForwardValidateIngesterRequest) (*apiv1.ForwardValidateIngesterResponse, error) {
	fn, ok := m.byNode[nodeID]
	if !ok {
		return nil, fmt.Errorf("no such node %q", nodeID)
	}
	success, msg := fn(ctx, req.GetType(), req.GetParams(), req.GetId())
	return &apiv1.ForwardValidateIngesterResponse{Success: success, Message: msg}, nil
}

type ingesterCheckCluster struct {
	serverByNode map[string]*server.SystemServer
	idByName     map[string]glid.GLID
	cfgStore     system.Store
}

// newIngesterCheckCluster stands up one real SystemServer per named node,
// sharing a replicated-config stand-in, each able to ask the others.
// sameHost models the dev-cluster shape (several nodes on one machine);
// distinctHosts models a normal multi-host cluster. The harness runs every node
// in ONE process either way, so distinctHosts is a deliberate double: it is what
// lets the trial-bind path be exercised at all. Its cost is that concurrent
// probes of one address still contend in-process, which is why the free-port
// case below assigns a single node.
type topologyMode int

const (
	distinctHosts topologyMode = iota
	sameHost
)

func newIngesterCheckCluster(t *testing.T, mode topologyMode, names ...string) *ingesterCheckCluster {
	t.Helper()
	ctx := context.Background()
	cfgStore := sysmem.NewStore()

	idByName := make(map[string]glid.GLID, len(names))
	for _, name := range names {
		id := glid.New()
		idByName[name] = id
		if err := cfgStore.PutNode(ctx, system.NodeConfig{
			ID: id, Name: name, State: system.NodeStateLive,
		}); err != nil {
			t.Fatalf("PutNode %s: %v", name, err)
		}
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{"memory": chunkmem.NewFactory()},
		IndexManagers: map[string]index.ManagerFactory{"memory": indexmem.NewFactory()},
		IngesterTypes: map[string]orchestrator.IngesterRegistration{
			"syslog": {Factory: ingestsyslog.NewFactory(), Defaults: ingestsyslog.ParamDefaults, ListenAddrs: ingestsyslog.ListenAddrs},
		},
	}

	ordered := make([]glid.GLID, len(names))
	for i, name := range names {
		ordered[i] = idByName[name]
	}
	topo := &mnTopology{ids: ordered, mode: mode}

	checkers := &mnIngesterCheckers{byNode: map[string]func(context.Context, string, map[string]string, []byte) (bool, string){}}
	serverByNode := make(map[string]*server.SystemServer, len(names))
	for _, name := range names {
		orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: idByName[name].String()})
		if err != nil {
			t.Fatalf("orchestrator.New %s: %v", name, err)
		}
		s := server.NewSystemServer(server.SystemServerConfig{
			Orch:                orch,
			CfgStore:            cfgStore,
			Factories:           factories,
			LocalNodeID:         idByName[name].String(),
			RemoteIngesterCheck: checkers,
			ClusterTopology:     topo,
		})
		serverByNode[name] = s
		checkers.byNode[idByName[name].String()] = s.LocalIngesterCheck
	}

	return &ingesterCheckCluster{serverByNode: serverByNode, idByName: idByName, cfgStore: cfgStore}
}

// holdPort binds addr for the duration of the test, standing in for an
// unrelated process already holding it on that node.
func holdPort(t *testing.T, addr string) string {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("cannot bind %s in this environment: %v", addr, err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

func testIngesterOn(ctx context.Context, s *server.SystemServer, params map[string]string, nodeIDs []glid.GLID, allNodes bool) (*apiv1.TestIngesterResponse, error) {
	raw := make([][]byte, len(nodeIDs))
	for i, id := range nodeIDs {
		raw[i] = []byte(id.String())
	}
	resp, err := s.TestIngester(ctx, connect.NewRequest(&apiv1.TestIngesterRequest{
		Type: "syslog", Params: params, NodeIds: raw, AllNodes: allNodes,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// THE REGRESSION. The node serving the request can bind the port; a node in the
// assignment cannot. Before the fix this answered "available" — the check only
// ever looked at whoever picked up.
func TestMultiNode_PortHeldOnAnAssignedNodeIsRejected(t *testing.T) {
	t.Parallel()
	c := newIngesterCheckCluster(t, distinctHosts, "coord", "data-1", "data-2")
	ctx := context.Background()

	// Bound in this process, which is every node's process in this harness —
	// so the port is genuinely unavailable to whichever node is asked, and the
	// assertion turns entirely on WHICH node is asked reporting it.
	addr := holdPort(t, "127.0.0.1:0")
	params := map[string]string{"tcp_addr": addr}
	// Guard against passing for the wrong reason: the config itself must be
	// valid, so a rejection can only come from the trial bind. An earlier draft
	// used the wrong param key, the factory refused to construct, and the test
	// "passed" without ever binding anything.
	if ok, msg := c.serverByNode["coord"].LocalIngesterCheck(ctx, "syslog", map[string]string{"tcp_addr": "127.0.0.1:0"}, nil); !ok {
		t.Fatalf("premise: this config must be constructible, got %q", msg)
	}

	resp, err := testIngesterOn(ctx, c.serverByNode["coord"], params,
		[]glid.GLID{c.idByName["data-1"], c.idByName["data-2"]}, false)
	if err != nil {
		t.Fatalf("TestIngester: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("reported available for an assignment that cannot bind it — the regression")
	}
	if len(resp.GetNodeChecks()) != 2 {
		t.Fatalf("got %d node checks, want one per assigned node", len(resp.GetNodeChecks()))
	}
	// The coordinator is not in the assignment, so it must not appear.
	for _, nc := range resp.GetNodeChecks() {
		if string(nc.GetNodeId()) == c.idByName["coord"].String() {
			t.Error("the responding node reported on itself despite not being assigned")
		}
	}
	if !strings.Contains(resp.GetMessage(), "data-1") {
		t.Errorf("message %q does not name an offending node", resp.GetMessage())
	}
}

// A free port must come back clear, so the test above is detecting the conflict
// rather than simply failing everything remote.
//
// Assigned to ONE node deliberately. The fan-out probes concurrently and every
// node in this harness shares a process, so two simultaneous trial binds of the
// same address collide with each other rather than with an outside holder.
// That is not only a harness artifact — co-located nodes are a real deployment
// (the dev cluster runs four on one host) — but it is a property of the address,
// not of this assertion, and conflating them would leave the fan-out untested
// for the case it exists to cover.
func TestMultiNode_FreePortIsAvailableThroughTheFanOut(t *testing.T) {
	t.Parallel()
	c := newIngesterCheckCluster(t, distinctHosts, "coord", "data-1", "data-2")
	ctx := context.Background()

	// Bind, read the assigned port, release: a port that was free a moment ago
	// and is free now.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	// data-1 only, and the request is served by coord — so the answer still
	// travels the remote path rather than being decided locally.
	resp, err := testIngesterOn(ctx, c.serverByNode["coord"], map[string]string{"tcp_addr": addr},
		[]glid.GLID{c.idByName["data-1"]}, false)
	if err != nil {
		t.Fatalf("TestIngester: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("free port reported unavailable: %s", resp.GetMessage())
	}
	if len(resp.GetNodeChecks()) != 1 || string(resp.GetNodeChecks()[0].GetNodeId()) != c.idByName["data-1"].String() {
		t.Errorf("node checks = %+v, want exactly data-1", resp.GetNodeChecks())
	}
}

// AllNodes must ask every member, including the one serving the request.
func TestMultiNode_AllNodesAsksEveryMemberIncludingTheResponder(t *testing.T) {
	t.Parallel()
	c := newIngesterCheckCluster(t, distinctHosts, "coord", "data-1", "data-2")
	ctx := context.Background()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	// Only the SET of nodes asked matters here, not the verdict: co-located
	// nodes probing one address collide with each other, which is the address's
	// problem and not this assertion's.
	resp, err := testIngesterOn(ctx, c.serverByNode["coord"], map[string]string{"tcp_addr": addr}, nil, true)
	if err != nil {
		t.Fatalf("TestIngester: %v", err)
	}
	if len(resp.GetNodeChecks()) != 3 {
		t.Fatalf("got %d node checks, want all 3 members", len(resp.GetNodeChecks()))
	}
	seen := map[string]bool{}
	for _, nc := range resp.GetNodeChecks() {
		seen[string(nc.GetNodeId())] = true
	}
	for name, id := range c.idByName {
		if !seen[id.String()] {
			t.Errorf("node %s was not asked under AllNodes", name)
		}
	}
}

// The verdict must not depend on which node the operator happens to reach —
// that arbitrariness was half the defect. Asking from a node inside the
// assignment and a node outside it must agree.
func TestMultiNode_VerdictIsIndependentOfWhichNodeAnswers(t *testing.T) {
	t.Parallel()
	c := newIngesterCheckCluster(t, distinctHosts, "coord", "data-1", "data-2")
	ctx := context.Background()

	addr := holdPort(t, "127.0.0.1:0")
	params := map[string]string{"tcp_addr": addr}
	// Guard against passing for the wrong reason: the config itself must be
	// valid, so a rejection can only come from the trial bind. An earlier draft
	// used the wrong param key, the factory refused to construct, and the test
	// "passed" without ever binding anything.
	if ok, msg := c.serverByNode["coord"].LocalIngesterCheck(ctx, "syslog", map[string]string{"tcp_addr": "127.0.0.1:0"}, nil); !ok {
		t.Fatalf("premise: this config must be constructible, got %q", msg)
	}
	assignment := []glid.GLID{c.idByName["data-1"], c.idByName["data-2"]}

	fromOutside, err := testIngesterOn(ctx, c.serverByNode["coord"], params, assignment, false)
	if err != nil {
		t.Fatalf("from coord: %v", err)
	}
	fromInside, err := testIngesterOn(ctx, c.serverByNode["data-1"], params, assignment, false)
	if err != nil {
		t.Fatalf("from data-1: %v", err)
	}
	if fromOutside.GetSuccess() != fromInside.GetSuccess() {
		t.Fatalf("verdict depends on who answered: coord=%v data-1=%v — same config, same assignment",
			fromOutside.GetSuccess(), fromInside.GetSuccess())
	}
	if len(fromOutside.GetNodeChecks()) != len(fromInside.GetNodeChecks()) {
		t.Errorf("node-check count differs by responder: %d vs %d",
			len(fromOutside.GetNodeChecks()), len(fromInside.GetNodeChecks()))
	}
}

// mnTopology reports node addresses. Under sameHost every node shares one — the
// literal truth in this harness, and a real deployment shape besides: the dev
// cluster runs four nodes on one machine.
type mnTopology struct {
	ids  []glid.GLID
	mode topologyMode
}

func (m *mnTopology) Servers() ([]cluster.RaftServer, error) {
	out := make([]cluster.RaftServer, len(m.ids))
	for i, id := range m.ids {
		host := fmt.Sprintf("10.0.0.%d", i+1)
		if m.mode == sameHost {
			host = "10.0.0.1"
		}
		out[i] = cluster.RaftServer{ID: id.String(), Address: fmt.Sprintf("%s:%d", host, 8300+i)}
	}
	return out, nil
}
func (m *mnTopology) LeaderInfo() (string, string)  { return "", "" }
func (m *mnTopology) LocalStats() map[string]string { return nil }
func (m *mnTopology) IsLeader() bool                { return false }
func (m *mnTopology) LeadershipTransfer() error     { return nil }

// The dev-cluster shape: several assigned nodes on one host. A listen address
// can only be held once there, so the config is impossible — and the answer
// must say that, the same way every time.
//
// Before this was decided from topology, the concurrent probes collided with
// each other: the verdict was right, but it blamed whichever node lost the
// race, and a rerun could blame a different one.
func TestMultiNode_CoLocatedNodesRejectListenerWithoutBlamingARaceLoser(t *testing.T) {
	t.Parallel()
	c := newIngesterCheckCluster(t, sameHost, "coord", "data-1", "data-2")
	ctx := context.Background()

	// A genuinely free port: the rejection must come from co-location, not from
	// anything being held.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	var messages []string
	for range 5 {
		resp, respErr := testIngesterOn(ctx, c.serverByNode["coord"], map[string]string{"tcp_addr": addr},
			[]glid.GLID{c.idByName["data-1"], c.idByName["data-2"]}, false)
		if respErr != nil {
			t.Fatalf("TestIngester: %v", respErr)
		}
		if resp.GetSuccess() {
			t.Fatal("accepted a listener for two nodes on one host; only one of them can hold the address")
		}
		if !strings.Contains(resp.GetMessage(), "10.0.0.1") {
			t.Errorf("message %q does not explain that the nodes share a host", resp.GetMessage())
		}
		if strings.Contains(resp.GetMessage(), "already in use") {
			t.Errorf("message %q still reports a bind conflict; the address is free, the assignment is the problem", resp.GetMessage())
		}
		messages = append(messages, resp.GetMessage())
	}
	for i, m := range messages {
		if m != messages[0] {
			t.Fatalf("verdict varies between runs:\n  run 0: %q\n  run %d: %q", messages[0], i, m)
		}
	}
}
