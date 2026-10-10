package server_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
)

// archiveRequestOfSize returns an ArchiveChunkRequest for vault that
// marshals to exactly n bytes, with the length of its chunk ID.
func archiveRequestOfSize(t *testing.T, vault string, n int) (*gastrologv1.ArchiveChunkRequest, int) {
	t.Helper()
	req := &gastrologv1.ArchiveChunkRequest{Vault: vault}
	for id := n - proto.Size(req); id > 0; id-- {
		req.ChunkId = bytes.Repeat([]byte{'c'}, id)
		if proto.Size(req) == n {
			return req, id
		}
	}
	t.Fatalf("no ArchiveChunkRequest marshals to exactly %d bytes", n)
	return nil, 0
}

// A request at the size limit the forwarding node accepts must reach the
// owner whole from every non-owner node: the owner's handler rejects the
// oversized chunk ID and its error names the exact length it received. One
// byte more is refused by the node the caller reached.
func TestForwardedRequestAtSizeLimitReachesOwnerFromEveryNode(t *testing.T) {
	nodeIDs := []string{"coord", "data-1", "data-2", "data-3"}
	const owner = "data-2"
	h := setupMultiNode(t, nodeIDs, WithoutVault("coord"))
	vault := h.Node(t, owner).vaultID.String()

	for _, from := range nodeIDs {
		if from == owner {
			continue
		}
		client := mnVaultClientFor(t, h, from)
		for _, n := range []int{
			cluster.ForwardRPCMaxResponseBytes - 1,
			cluster.ForwardRPCMaxResponseBytes,
		} {
			t.Run(fmt.Sprintf("%s/%d", from, n), func(t *testing.T) {
				req, idLen := archiveRequestOfSize(t, vault, n)
				_, err := client.ArchiveChunk(context.Background(), connect.NewRequest(req))
				got := connectErrorOf(t, err)
				want := fmt.Sprintf("invalid chunk_id: invalid chunk ID: expected 16 bytes, got %d", idLen)
				if got.Code() != connect.CodeInvalidArgument || got.Message() != want {
					t.Fatalf("%d-byte request from %s: %v %q, want invalid_argument %q",
						n, from, got.Code(), got.Message(), want)
				}
			})
		}
		t.Run(from+"/over", func(t *testing.T) {
			req, _ := archiveRequestOfSize(t, vault, cluster.ForwardRPCMaxResponseBytes+1)
			_, err := client.ArchiveChunk(context.Background(), connect.NewRequest(req))
			if code := connect.CodeOf(err); code != connect.CodeResourceExhausted {
				t.Fatalf("over-limit request from %s: code %v (%v), want resource_exhausted", from, code, err)
			}
		})
	}
}
