package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// fakeRemoteCheck answers for peers without a cluster. verdicts maps node ID to
// (success, message); a node in errs fails the call instead of answering, which
// is a different thing and must be reported differently.
type fakeRemoteCheck struct {
	verdicts map[string]bool
	errs     map[string]error

	// The fan-out is concurrent, so recording who was asked needs a lock. The
	// production side does not: each goroutine writes its own index in a
	// pre-sized slice.
	mu    sync.Mutex
	asked []string
}

func (f *fakeRemoteCheck) ValidateIngester(_ context.Context, nodeID string, _ *apiv1.ForwardValidateIngesterRequest) (*apiv1.ForwardValidateIngesterResponse, error) {
	f.mu.Lock()
	f.asked = append(f.asked, nodeID)
	f.mu.Unlock()
	if err, ok := f.errs[nodeID]; ok {
		return nil, err
	}
	ok := f.verdicts[nodeID]
	msg := "listen addresses available"
	if !ok {
		msg = "tcp :514: bind: address already in use"
	}
	return &apiv1.ForwardValidateIngesterResponse{Success: ok, Message: msg}, nil
}

func nodeStore(t *testing.T, nodes map[string]system.NodeState) (*sysmem.Store, map[string]glid.GLID) {
	t.Helper()
	store := sysmem.NewStore()
	ids := map[string]glid.GLID{}
	for name, state := range nodes {
		id := glid.New()
		ids[name] = id
		if err := store.PutNode(context.Background(), system.NodeConfig{
			ID: id, Name: name, State: state,
		}); err != nil {
			t.Fatalf("PutNode %s: %v", name, err)
		}
	}
	return store, ids
}

// --- target resolution ---

func TestIngesterCheckTargets_AllNodesAsksEveryServiceableNode(t *testing.T) {
	t.Parallel()
	store, ids := nodeStore(t, map[string]system.NodeState{
		"n1": system.NodeStateLive,
		"n2": system.NodeStateLive,
		// Out of service: it cannot answer, and its verdict would gate nothing
		// because the ingester will not start there until it returns — at which
		// point the config is validated again.
		"n3": system.NodeStateUnreachable,
		"n4": system.NodeStateDecommissioning,
		// Operator-quiesced but reachable, so it can still answer.
		"n5": system.NodeStateMaintenance,
	})
	s := &SystemServer{sysStore: store, localNodeID: ids["n1"].String()}

	got := s.ingesterCheckTargets(context.Background(), nil, true)
	want := map[string]bool{ids["n1"].String(): true, ids["n2"].String(): true, ids["n5"].String(): true}
	if len(got) != len(want) {
		t.Fatalf("targets = %v (%d), want the %d serviceable nodes", got, len(got), len(want))
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("asked %s, which is out of service", g)
		}
	}
}

func TestIngesterCheckTargets_AllNodesWinsOverNodeIDs(t *testing.T) {
	t.Parallel()
	store, ids := nodeStore(t, map[string]system.NodeState{"n1": system.NodeStateLive, "n2": system.NodeStateLive})
	s := &SystemServer{sysStore: store, localNodeID: ids["n1"].String()}

	// The orchestrator treats AllNodes as making every node eligible regardless
	// of NodeIDs; the check has to agree or it would validate a narrower set
	// than the one the ingester runs on.
	got := s.ingesterCheckTargets(context.Background(), []string{ids["n1"].String()}, true)
	if len(got) != 2 {
		t.Fatalf("targets = %v, want both nodes: AllNodes must override a stale NodeIDs list", got)
	}
}

func TestIngesterCheckTargets_ExplicitAssignmentIsAskedVerbatim(t *testing.T) {
	t.Parallel()
	store, ids := nodeStore(t, map[string]system.NodeState{"n1": system.NodeStateLive, "n2": system.NodeStateLive, "n3": system.NodeStateLive})
	s := &SystemServer{sysStore: store, localNodeID: ids["n1"].String()}

	// The regression case: an assignment that does NOT include the node handling
	// the request. Previously this produced no check at all.
	got := s.ingesterCheckTargets(context.Background(), []string{ids["n2"].String(), ids["n3"].String()}, false)
	if len(got) != 2 || got[0] == s.localNodeID || got[1] == s.localNodeID {
		t.Fatalf("targets = %v, want exactly the two assigned nodes and not the local one", got)
	}
}

func TestIngesterCheckTargets_UnassignedFormChecksTheRespondingNode(t *testing.T) {
	t.Parallel()
	store, ids := nodeStore(t, map[string]system.NodeState{"n1": system.NodeStateLive})
	s := &SystemServer{sysStore: store, localNodeID: ids["n1"].String()}

	// A form the operator has not assigned yet still deserves an answer, and
	// the only defensible one is about this node.
	got := s.ingesterCheckTargets(context.Background(), nil, false)
	if len(got) != 1 || got[0] != ids["n1"].String() {
		t.Fatalf("targets = %v, want just the local node", got)
	}
}

// --- fan-out ---

// The regression, end to end at this layer: an ingester assigned away from the
// responding node, with the port held on one of the assigned nodes, must come
// back rejected and name that node.
func TestCheckIngesterOnNodes_RejectsWhenAnAssignedRemoteNodeCannotBind(t *testing.T) {
	t.Parallel()
	remote := &fakeRemoteCheck{verdicts: map[string]bool{"n2": true, "n3": false}}
	s := &SystemServer{localNodeID: "n1", remoteIngesterCheck: remote}

	checks := s.checkIngesterOnNodes(context.Background(), "syslog", map[string]string{"address": ":514"}, nil, []string{"n2", "n3"})
	if len(checks) != 2 {
		t.Fatalf("got %d checks, want one per assigned node", len(checks))
	}
	ok, msg := summarizeIngesterChecks(checks, func(id string) string { return id })
	if ok {
		t.Fatal("accepted a config that cannot bind on an assigned node — this is the regression")
	}
	if !strings.Contains(msg, "n3") {
		t.Errorf("message %q does not name the offending node; on a cluster that is unactionable", msg)
	}
	if strings.Contains(msg, "n2") {
		t.Errorf("message %q blames a node that answered successfully", msg)
	}
}

func TestCheckIngesterOnNodes_UnreachableIsNotAFailedCheck(t *testing.T) {
	t.Parallel()
	remote := &fakeRemoteCheck{
		verdicts: map[string]bool{"n2": true},
		errs:     map[string]error{"n3": errors.New("connection refused")},
	}
	s := &SystemServer{localNodeID: "n1", remoteIngesterCheck: remote}

	checks := s.checkIngesterOnNodes(context.Background(), "syslog", nil, nil, []string{"n2", "n3"})
	var n3 *apiv1.IngesterNodeCheck
	for _, c := range checks {
		if string(c.GetNodeId()) == "n3" {
			n3 = c
		}
	}
	if n3 == nil {
		t.Fatal("no result recorded for the unreachable node; a silent omission reads as success")
	}
	if !n3.GetUnreachable() {
		t.Error("a node that did not answer was recorded as a verdict rather than as unreachable")
	}

	// It must not block the save: the operator cannot fix another node's
	// reachability from this form, and the config may be perfectly valid.
	ok, msg := summarizeIngesterChecks(checks, func(id string) string { return id })
	if !ok {
		t.Errorf("an unreachable node blocked a valid config: %q", msg)
	}
	if !strings.Contains(msg, "n3") {
		t.Errorf("message %q hides that a node went unchecked", msg)
	}
}

func TestCheckIngesterOnNodes_NoForwarderReportsUnreachableNotSuccess(t *testing.T) {
	t.Parallel()
	// Single-node build asked about a peer. Claiming success would be a lie
	// about a node it never contacted.
	s := &SystemServer{localNodeID: "n1"}
	checks := s.checkIngesterOnNodes(context.Background(), "syslog", nil, nil, []string{"n2"})
	if len(checks) != 1 {
		t.Fatalf("got %d checks, want 1", len(checks))
	}
	if checks[0].GetSuccess() || !checks[0].GetUnreachable() {
		t.Errorf("check = %+v, want unreachable and not success", checks[0])
	}
}

func TestCheckIngesterOnNodes_AsksEveryAssignedNodeOnce(t *testing.T) {
	t.Parallel()
	remote := &fakeRemoteCheck{verdicts: map[string]bool{"n2": true, "n3": true, "n4": true}}
	s := &SystemServer{localNodeID: "n1", remoteIngesterCheck: remote}

	s.checkIngesterOnNodes(context.Background(), "syslog", nil, nil, []string{"n2", "n3", "n4"})
	remote.mu.Lock()
	asked := append([]string(nil), remote.asked...)
	remote.mu.Unlock()
	if len(asked) != 3 {
		t.Fatalf("asked %v, want exactly one call per assigned node", asked)
	}
}

// --- summary policy ---

func TestSummarizeIngesterChecks_UsesNodeNamesNotIDs(t *testing.T) {
	t.Parallel()
	checks := []*apiv1.IngesterNodeCheck{
		{NodeId: []byte("01hxyz"), Success: false, Message: "bind: address already in use"},
	}
	_, msg := summarizeIngesterChecks(checks, func(id string) string {
		if id == "01hxyz" {
			return "edge-2"
		}
		return id
	})
	if !strings.Contains(msg, "edge-2") {
		t.Errorf("message %q uses the raw ID; the operator would have to go look it up", msg)
	}
}

func TestSummarizeIngesterChecks_SingleNodeKeepsTheNodesOwnMessage(t *testing.T) {
	t.Parallel()
	checks := []*apiv1.IngesterNodeCheck{{NodeId: []byte("n1"), Success: true, Message: "listen addresses available"}}
	ok, msg := summarizeIngesterChecks(checks, func(id string) string { return id })
	if !ok || msg != "listen addresses available" {
		t.Errorf("got (%v, %q); a single-node answer should read as it always did", ok, msg)
	}
}

func TestSummarizeIngesterChecks_FailureWinsOverUnreachable(t *testing.T) {
	t.Parallel()
	// A definite "cannot bind here" is actionable now; an unanswered node is
	// not. Reporting the actionable one is more useful than reporting both.
	checks := []*apiv1.IngesterNodeCheck{
		{NodeId: []byte("n2"), Unreachable: true},
		{NodeId: []byte("n3"), Success: false, Message: "bind: address already in use"},
	}
	ok, msg := summarizeIngesterChecks(checks, func(id string) string { return id })
	if ok {
		t.Fatal("a definite failure must block regardless of an unreachable peer")
	}
	if !strings.Contains(msg, "n3") {
		t.Errorf("message %q does not lead with the actionable failure", msg)
	}
}

// fakeTopology reports node addresses so co-location can be decided without a
// cluster.
type fakeTopology struct{ addrs map[string]string }

func (f *fakeTopology) Servers() ([]cluster.RaftServer, error) {
	out := make([]cluster.RaftServer, 0, len(f.addrs))
	for id, addr := range f.addrs {
		out = append(out, cluster.RaftServer{ID: id, Address: addr})
	}
	return out, nil
}
func (f *fakeTopology) LeaderInfo() (string, string)  { return "", "" }
func (f *fakeTopology) LocalStats() map[string]string { return nil }
func (f *fakeTopology) IsLeader() bool                { return false }
func (f *fakeTopology) LeadershipTransfer() error     { return nil }

func listenerFactories() orchestrator.Factories {
	return orchestrator.Factories{
		IngesterTypes: map[string]orchestrator.IngesterRegistration{
			"syslog": {ListenAddrs: func(map[string]string) []ingestion.ListenAddr { return nil }},
			"kafka":  {}, // no ListenAddrs: not a listener
		},
	}
}

// Two assigned nodes on one host cannot both hold a listen address. Deciding it
// from topology rather than from a bind race is what makes the answer the same
// on every run — previously the losers of the race were blamed, and which nodes
// those were varied.
func TestCoLocatedListenerChecks_SharedHostIsSettledWithoutProbing(t *testing.T) {
	t.Parallel()
	s := &SystemServer{
		localNodeID: "n1",
		factories:   listenerFactories(),
		clusterTopology: &fakeTopology{addrs: map[string]string{
			"n1": "10.0.0.1:8300",
			"n2": "10.0.0.1:8301", // same host as n1
			"n3": "10.0.0.2:8300",
		}},
	}

	settled, remaining := s.coLocatedListenerChecks("syslog", []string{"n1", "n2", "n3"}, func(id string) string { return id })
	if len(settled) != 2 {
		t.Fatalf("settled %d, want both co-located nodes decided up front", len(settled))
	}
	for _, c := range settled {
		if c.GetSuccess() {
			t.Errorf("node %s reported success despite sharing a host", c.GetNodeId())
		}
		if !strings.Contains(c.GetMessage(), "10.0.0.1") {
			t.Errorf("message %q does not name the shared host", c.GetMessage())
		}
	}
	if len(remaining) != 1 || remaining[0] != "n3" {
		t.Fatalf("remaining = %v, want only the node that has its host to itself", remaining)
	}
}

// Determinism is the point: the same inputs must produce the same verdict and
// the same blame, run after run. The old behaviour depended on which concurrent
// bind happened to win.
func TestCoLocatedListenerChecks_IsDeterministic(t *testing.T) {
	t.Parallel()
	s := &SystemServer{
		localNodeID: "n1",
		factories:   listenerFactories(),
		clusterTopology: &fakeTopology{addrs: map[string]string{
			"n1": "10.0.0.1:8300", "n2": "10.0.0.1:8301", "n3": "10.0.0.1:8302",
		}},
	}
	var first string
	for range 20 {
		settled, _ := s.coLocatedListenerChecks("syslog", []string{"n1", "n2", "n3"}, func(id string) string { return id })
		_, msg := summarizeIngesterChecks(settled, func(id string) string { return id })
		if first == "" {
			first = msg
			continue
		}
		if msg != first {
			t.Fatalf("verdict varies between runs:\n  %q\n  %q", first, msg)
		}
	}
}

func TestCoLocatedListenerChecks_DistinctHostsAreAllProbed(t *testing.T) {
	t.Parallel()
	s := &SystemServer{
		localNodeID: "n1",
		factories:   listenerFactories(),
		clusterTopology: &fakeTopology{addrs: map[string]string{
			"n1": "10.0.0.1:8300", "n2": "10.0.0.2:8300", "n3": "10.0.0.3:8300",
		}},
	}
	settled, remaining := s.coLocatedListenerChecks("syslog", []string{"n1", "n2", "n3"}, func(id string) string { return id })
	if len(settled) != 0 {
		t.Errorf("settled %d without probing; nothing is co-located here", len(settled))
	}
	if len(remaining) != 3 {
		t.Errorf("remaining = %v, want all three probed", remaining)
	}
}

// A non-listener binds nothing, so sharing a host is irrelevant to it.
func TestCoLocatedListenerChecks_NonListenerIsUnaffected(t *testing.T) {
	t.Parallel()
	s := &SystemServer{
		localNodeID: "n1",
		factories:   listenerFactories(),
		clusterTopology: &fakeTopology{addrs: map[string]string{
			"n1": "10.0.0.1:8300", "n2": "10.0.0.1:8301",
		}},
	}
	settled, remaining := s.coLocatedListenerChecks("kafka", []string{"n1", "n2"}, func(id string) string { return id })
	if len(settled) != 0 || len(remaining) != 2 {
		t.Errorf("settled=%d remaining=%v; a non-listener must still be asked", len(settled), remaining)
	}
}

// Without topology there is nothing to decide from, so every node is probed
// rather than guessed about.
func TestCoLocatedListenerChecks_NoTopologyProbesEveryone(t *testing.T) {
	t.Parallel()
	s := &SystemServer{localNodeID: "n1", factories: listenerFactories()}
	settled, remaining := s.coLocatedListenerChecks("syslog", []string{"n1", "n2"}, func(id string) string { return id })
	if len(settled) != 0 || len(remaining) != 2 {
		t.Errorf("settled=%d remaining=%v; with no addresses, probe rather than assume", len(settled), remaining)
	}
}
