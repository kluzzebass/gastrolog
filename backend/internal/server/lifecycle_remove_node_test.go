package server_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"
)

// The RemoveNode RPC is where the removal policy is decided
// (gastrolog-3vyex): allow_self against this node is the preStop
// `cluster demote-self` path and gets the optimistic RF stance;
// everything else is operator-driven and gets the pessimistic one. These
// tests pin that derivation and the error classification, with the
// leader-side gates stubbed — the gates themselves are tested in
// internal/app.

type removeNodeRecorder struct {
	mu    sync.Mutex
	calls []removeNodeCall
	err   error
}

type removeNodeCall struct {
	nodeID string
	opts   cluster.RemoveNodeOptions
}

func (r *removeNodeRecorder) fn(_ context.Context, nodeID string, opts cluster.RemoveNodeOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, removeNodeCall{nodeID: nodeID, opts: opts})
	return r.err
}

func (r *removeNodeRecorder) only(t *testing.T) removeNodeCall {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) != 1 {
		t.Fatalf("expected exactly one removal call, got %d", len(r.calls))
	}
	return r.calls[0]
}

// setupRemoveNodeTest builds a lifecycle server whose node ID is a real
// GLID, with the removal callback replaced by a recorder.
func setupRemoveNodeTest(t *testing.T, rec *removeNodeRecorder) (gastrologv1connect.LifecycleServiceClient, string) {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	nodeID := glid.New().String()
	srv := server.New(orch, sysmem.NewStore(), orchestrator.Factories{}, nil, server.Config{
		NodeID:         nodeID,
		RemoveNodeFunc: rec.fn,
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded"), nodeID
}

// An operator removing some other node gets the pessimistic policy.
func TestRemoveNode_OperatorPolicyByDefault(t *testing.T) {
	t.Parallel()
	rec := &removeNodeRecorder{}
	client, _ := setupRemoveNodeTest(t, rec)

	target := glid.New().String()
	if _, err := client.RemoveNode(context.Background(), connect.NewRequest(&gastrologv1.RemoveNodeRequest{
		NodeId: []byte(target),
	})); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	call := rec.only(t)
	if call.nodeID != target {
		t.Fatalf("target: got %s, want %s", call.nodeID, target)
	}
	if call.opts.Policy != cluster.RemovalPolicyOperator {
		t.Fatalf("policy: got %v, want operator", call.opts.Policy)
	}
	if call.opts.Force {
		t.Fatal("force must default to false")
	}
}

// preStop `cluster demote-self` — allow_self against this very node —
// gets the optimistic policy.
func TestRemoveNode_SelfRemovalGetsSelfPolicy(t *testing.T) {
	t.Parallel()
	rec := &removeNodeRecorder{}
	client, nodeID := setupRemoveNodeTest(t, rec)

	if _, err := client.RemoveNode(context.Background(), connect.NewRequest(&gastrologv1.RemoveNodeRequest{
		NodeId:    []byte(nodeID),
		AllowSelf: true,
	})); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	if got := rec.only(t).opts.Policy; got != cluster.RemovalPolicySelf {
		t.Fatalf("policy: got %v, want self", got)
	}
}

// allow_self is not a general opt-out of the operator stance: setting it
// while targeting a DIFFERENT node is still an operator-driven removal,
// so the pessimistic gate applies.
func TestRemoveNode_AllowSelfTargetingOtherNodeStaysOperator(t *testing.T) {
	t.Parallel()
	rec := &removeNodeRecorder{}
	client, _ := setupRemoveNodeTest(t, rec)

	if _, err := client.RemoveNode(context.Background(), connect.NewRequest(&gastrologv1.RemoveNodeRequest{
		NodeId:    []byte(glid.New().String()),
		AllowSelf: true,
	})); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	if got := rec.only(t).opts.Policy; got != cluster.RemovalPolicyOperator {
		t.Fatalf("policy: got %v, want operator", got)
	}
}

// The force flag reaches the gates.
func TestRemoveNode_ForcePassedThrough(t *testing.T) {
	t.Parallel()
	rec := &removeNodeRecorder{}
	client, _ := setupRemoveNodeTest(t, rec)

	if _, err := client.RemoveNode(context.Background(), connect.NewRequest(&gastrologv1.RemoveNodeRequest{
		NodeId: []byte(glid.New().String()),
		Force:  true,
	})); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	if !rec.only(t).opts.Force {
		t.Fatal("force flag did not reach the removal callback")
	}
}

// Self-removal without allow_self is still blocked by the typo guard
// before any policy is chosen (gastrolog-24iv4, unchanged).
func TestRemoveNode_SelfWithoutAllowSelfRejected(t *testing.T) {
	t.Parallel()
	rec := &removeNodeRecorder{}
	client, nodeID := setupRemoveNodeTest(t, rec)

	_, err := client.RemoveNode(context.Background(), connect.NewRequest(&gastrologv1.RemoveNodeRequest{
		NodeId: []byte(nodeID),
	}))
	if err == nil {
		t.Fatal("expected self-removal without allow_self to be rejected")
	}
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s: %v", got, err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.calls) != 0 {
		t.Fatalf("removal callback must not run, got %v", rec.calls)
	}
}

// Both removal-gate refusals are operator-correctable, so they surface
// as FailedPrecondition rather than Internal — the CLI and UI treat
// those differently.
func TestRemoveNode_GateRefusalIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	for name, gateErr := range map[string]error{
		"orphan": errors.New(`refusing to remove node n1: would orphan 1 vault(s): "logs" (v1) — drain these vaults to other nodes first, or re-run with --force to acknowledge data loss`),
		"rf":     errors.New(`refusing to remove node n1: removal would drop a vault below its replication factor — 1 vault(s) affected: "logs" (v1): 2 of 3 replicas would survive, 0 eligible node(s) to re-place onto — add an eligible node or drain these vaults first, or re-run with --force to accept reduced redundancy`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &removeNodeRecorder{err: gateErr}
			client, _ := setupRemoveNodeTest(t, rec)

			_, err := client.RemoveNode(context.Background(), connect.NewRequest(&gastrologv1.RemoveNodeRequest{
				NodeId: []byte(glid.New().String()),
			}))
			if err == nil {
				t.Fatal("expected gate refusal to surface as an error")
			}
			if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
				t.Fatalf("expected FailedPrecondition, got %s: %v", got, err)
			}
			if !strings.Contains(err.Error(), "logs") {
				t.Fatalf("refusal must reach the caller naming the vault: %v", err)
			}
		})
	}
}

// A genuine failure inside the removal is still Internal — the
// FailedPrecondition mapping must not swallow real errors.
func TestRemoveNode_InternalErrorStaysInternal(t *testing.T) {
	t.Parallel()
	rec := &removeNodeRecorder{err: errors.New("remove server: leadership lost")}
	client, _ := setupRemoveNodeTest(t, rec)

	_, err := client.RemoveNode(context.Background(), connect.NewRequest(&gastrologv1.RemoveNodeRequest{
		NodeId: []byte(glid.New().String()),
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	if got := connect.CodeOf(err); got != connect.CodeInternal {
		t.Fatalf("expected Internal, got %s: %v", got, err)
	}
}
