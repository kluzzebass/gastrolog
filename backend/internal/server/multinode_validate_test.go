package server_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/orchestrator"
)

// A node that holds no instance of the vault must still answer validate by
// asking the nodes that do, rather than refusing with "vault not ready".
func TestMultiNode_ValidateFromANodeWithoutTheVaultReportsThePeersCopy(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))
	data := h.Node(t, "data-1")
	addMNRecordsAt(t, data, "r", 25, time.Now().Add(-time.Minute))
	// Config reload registers every configured vault on every node; a node
	// the vault is not placed on ends up with an entry and no instance.
	coord := h.Node(t, "coord")
	coord.orch.RegisterVault(&orchestrator.Vault{ID: data.vaultID, Name: "vault-data-1"})
	if _, err := coord.orch.ListLocalChunkMetas(data.vaultID); !errors.Is(err, orchestrator.ErrVaultNotReady) {
		t.Fatalf("premise: the coordinator should know the vault but hold no instance, got %v", err)
	}

	resp, err := h.vaultClient.ValidateVault(context.Background(), connect.NewRequest(&gastrologv1.ValidateVaultRequest{Vault: data.vaultID.String()}))
	if err != nil {
		t.Fatalf("validate from a node without the vault: %v", err)
	}
	if !resp.Msg.GetValid() {
		t.Fatalf("healthy remote vault reported invalid: %v", resp.Msg)
	}
	if len(resp.Msg.GetChunks()) == 0 {
		t.Fatal("no chunk validations came back from the node holding the vault")
	}
	for _, cv := range resp.Msg.GetChunks() {
		if cv.GetNodeId() != "data-1" {
			t.Fatalf("chunk validation attributed to %q, want data-1", cv.GetNodeId())
		}
	}
	if len(resp.Msg.GetContributionReport().GetDegraded()) != 0 {
		t.Fatalf("fan-out reported degraded peers: %v", resp.Msg.GetContributionReport())
	}
}
