package server_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// setupSetNodeStateTest builds a single-node lifecycle server backed
// by an in-memory cfgStore seeded with one NodeConfig at the given
// state. Returns the connect client + the node's GLID.
func setupSetNodeStateTest(t *testing.T, initialState system.NodeState) (gastrologv1connect.LifecycleServiceClient, glid.GLID) {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	store := sysmem.NewStore()
	nodeID := glid.New()
	if err := store.PutNode(context.Background(), system.NodeConfig{
		ID: nodeID, Name: "node-1", State: initialState, StateSince: time.Now(),
	}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	srv := server.New(orch, store, orchestrator.Factories{}, nil, server.Config{
		NodeID: nodeID.String(),
	})
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: srv.Handler()},
	}
	return gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded"), nodeID
}

func TestSetNodeState_LiveToMaintenance(t *testing.T) {
	t.Parallel()
	client, nodeID := setupSetNodeStateTest(t, system.NodeStateLive)
	_, err := client.SetNodeState(context.Background(), connect.NewRequest(&gastrologv1.SetNodeStateRequest{
		NodeId: []byte(nodeID.String()),
		State:  gastrologv1.NodeState_NODE_STATE_MAINTENANCE,
	}))
	if err != nil {
		t.Fatalf("Live → Maintenance: %v", err)
	}
}

// TestSetNodeState_Idempotent verifies that re-applying the same
// state is a no-op success (per ValidateNodeStateTransition's
// idempotent contract). Operators expect re-running `cluster
// maintenance N` against an already-Maintenance node to succeed, not
// fail with "illegal transition".
func TestSetNodeState_Idempotent(t *testing.T) {
	t.Parallel()
	client, nodeID := setupSetNodeStateTest(t, system.NodeStateMaintenance)
	_, err := client.SetNodeState(context.Background(), connect.NewRequest(&gastrologv1.SetNodeStateRequest{
		NodeId: []byte(nodeID.String()),
		State:  gastrologv1.NodeState_NODE_STATE_MAINTENANCE,
	}))
	if err != nil {
		t.Fatalf("Maintenance → Maintenance (idempotent): %v", err)
	}
}

// TestSetNodeState_IllegalTransition verifies an illegal lifecycle
// transition surfaces as FailedPrecondition (not Internal), so the
// CLI can distinguish operator-fixable errors from genuine failures.
// Maintenance → Unreachable is illegal: operator-set states do not
// auto-downgrade.
func TestSetNodeState_IllegalTransition(t *testing.T) {
	t.Parallel()
	client, nodeID := setupSetNodeStateTest(t, system.NodeStateMaintenance)
	_, err := client.SetNodeState(context.Background(), connect.NewRequest(&gastrologv1.SetNodeStateRequest{
		NodeId: []byte(nodeID.String()),
		State:  gastrologv1.NodeState_NODE_STATE_UNREACHABLE,
	}))
	if err == nil {
		t.Fatal("expected error for Maintenance → Unreachable, got nil")
	}
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %s: %v", got, err)
	}
}

// TestSetNodeState_UnknownNode verifies a non-existent node ID also
// surfaces as FailedPrecondition (operator-fixable: pick the right
// name).
func TestSetNodeState_UnknownNode(t *testing.T) {
	t.Parallel()
	client, _ := setupSetNodeStateTest(t, system.NodeStateLive)
	bogus := glid.New()
	_, err := client.SetNodeState(context.Background(), connect.NewRequest(&gastrologv1.SetNodeStateRequest{
		NodeId: []byte(bogus.String()),
		State:  gastrologv1.NodeState_NODE_STATE_MAINTENANCE,
	}))
	if err == nil {
		t.Fatal("expected error for unknown node, got nil")
	}
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %s: %v", got, err)
	}
}

// TestSetNodeState_RejectsUnspecifiedState verifies the RPC rejects
// the proto zero-value (UNSPECIFIED) at the boundary rather than
// letting it through as the legacy/lazy-migration Unknown state. The
// FSM treats UNSPECIFIED as Live via EffectiveState(), but operator-
// driven transitions must be explicit.
func TestSetNodeState_RejectsUnspecifiedState(t *testing.T) {
	t.Parallel()
	client, nodeID := setupSetNodeStateTest(t, system.NodeStateLive)
	_, err := client.SetNodeState(context.Background(), connect.NewRequest(&gastrologv1.SetNodeStateRequest{
		NodeId: []byte(nodeID.String()),
		State:  gastrologv1.NodeState_NODE_STATE_UNSPECIFIED,
	}))
	if err == nil {
		t.Fatal("expected error for UNSPECIFIED state, got nil")
	}
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s: %v", got, err)
	}
}

// TestSetNodeState_DrainToLiveCancelsDrain exercises the cancel-
// drain path the CLI verb wraps: Draining → Live is a legal
// transition specifically to support operator override before
// decommission completes.
func TestSetNodeState_DrainToLiveCancelsDrain(t *testing.T) {
	t.Parallel()
	client, nodeID := setupSetNodeStateTest(t, system.NodeStateDraining)
	_, err := client.SetNodeState(context.Background(), connect.NewRequest(&gastrologv1.SetNodeStateRequest{
		NodeId: []byte(nodeID.String()),
		State:  gastrologv1.NodeState_NODE_STATE_LIVE,
	}))
	if err != nil {
		t.Fatalf("Draining → Live (cancel drain): %v", err)
	}
}
