package server_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

// A resource-owner RPC that fails on the owner must fail identically on every
// node: the caller on a non-owner node gets the owner handler's exact Connect
// code and bare message, as if it had called the owner itself.

// mnVaultClientFor builds a VaultService client whose server runs AS nodeID,
// forwarding to the other harness nodes over the production ForwardRPC path.
func mnVaultClientFor(t *testing.T, h *multiNodeHarness, nodeID string) gastrologv1connect.VaultServiceClient {
	t.Helper()
	node := h.Node(t, nodeID)
	srv := server.New(node.orch, h.store(t, nodeID), orchestrator.Factories{VaultsDir: t.TempDir()}, nil, server.Config{
		NoAuth:           true,
		NodeID:           nodeID,
		RoutingForwarder: newClusterForwarder(t, h.nodes, nodeID, t.TempDir()),
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewVaultServiceClient(httpClient, "http://embedded")
}

func TestForwardedOwnerErrorMatchesOwnerFromEveryNode(t *testing.T) {
	nodeIDs := []string{"coord", "data-1", "data-2", "data-3"}
	const owner = "data-2"
	h := setupMultiNode(t, nodeIDs, WithoutVault("coord"))

	ownerNode := h.Node(t, owner)
	addMNRecords(t, ownerNode, "open", 3, nil)
	active := ownerNode.vault.CM.Active()
	if active == nil {
		t.Fatal("owner vault has no active chunk after appending records")
	}
	vault := ownerNode.vaultID.String()
	ghostChunk := glid.New().Bytes()

	clients := make(map[string]gastrologv1connect.VaultServiceClient, len(nodeIDs))
	for _, id := range nodeIDs {
		clients[id] = mnVaultClientFor(t, h, id)
	}

	cases := []struct {
		name     string
		wantCode connect.Code
		call     func(context.Context, gastrologv1connect.VaultServiceClient) error
	}{
		{
			// FailedPrecondition shares HTTP 400 with InvalidArgument and
			// OutOfRange: only the decoded body tells them apart.
			name:     "failed precondition",
			wantCode: connect.CodeFailedPrecondition,
			call: func(ctx context.Context, c gastrologv1connect.VaultServiceClient) error {
				_, err := c.RepatriateOrphan(ctx, connect.NewRequest(&gastrologv1.RepatriateOrphanRequest{
					Vault: vault, ChunkId: glid.GLID(active.ID).Bytes(),
				}))
				return err
			},
		},
		{
			name:     "not found",
			wantCode: connect.CodeNotFound,
			call: func(ctx context.Context, c gastrologv1connect.VaultServiceClient) error {
				_, err := c.RepatriateOrphan(ctx, connect.NewRequest(&gastrologv1.RepatriateOrphanRequest{
					Vault: vault, ChunkId: ghostChunk,
				}))
				return err
			},
		},
		{
			name:     "invalid argument",
			wantCode: connect.CodeInvalidArgument,
			call: func(ctx context.Context, c gastrologv1connect.VaultServiceClient) error {
				_, err := c.ArchiveChunk(ctx, connect.NewRequest(&gastrologv1.ArchiveChunkRequest{
					Vault: vault, ChunkId: glid.GLID(active.ID).Bytes(),
				}))
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			want := connectErrorOf(t, tc.call(ctx, clients[owner]))
			if want.Code() != tc.wantCode {
				t.Fatalf("owner %s answered %v %q, want code %v", owner, want.Code(), want.Message(), tc.wantCode)
			}
			for _, from := range nodeIDs {
				if from == owner {
					continue
				}
				got := connectErrorOf(t, tc.call(ctx, clients[from]))
				if got.Code() != want.Code() {
					t.Errorf("from %s: code %v, owner answered %v", from, got.Code(), want.Code())
				}
				if got.Message() != want.Message() {
					t.Errorf("from %s: message %q, owner answered %q", from, got.Message(), want.Message())
				}
			}
		})
	}
}

func connectErrorOf(t *testing.T, err error) *connect.Error {
	t.Helper()
	if err == nil {
		t.Fatal("expected the owner's handler to fail, got success")
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("error %v (%T) is not a Connect error", err, err)
	}
	return ce
}
