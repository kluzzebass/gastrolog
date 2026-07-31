package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
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
