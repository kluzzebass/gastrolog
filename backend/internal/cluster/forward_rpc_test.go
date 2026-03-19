package cluster_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// TestConnectMuxDispatch verifies that we can dispatch a raw HTTP request
// through a Connect mux and get a valid proto response. This is the exact
// mechanism ForwardRPC uses internally.
func TestConnectMuxDispatch(t *testing.T) {
	// Build a Connect mux with a ListChunks handler that returns known data.
	mux := http.NewServeMux()
	mux.Handle(gastrologv1connect.NewVaultServiceHandler(&fakeVaultService{
		chunks: []*apiv1.ChunkMeta{
			{Id: "chunk-1", RecordCount: 100},
			{Id: "chunk-2", RecordCount: 200},
		},
	}))

	// Serialize a ListChunksRequest.
	reqMsg := &apiv1.ListChunksRequest{Vault: "test-vault-id"}
	reqBytes, err := proto.Marshal(reqMsg)
	if err != nil {
		t.Fatal(err)
	}

	// Dispatch through the mux exactly as ForwardRPC does.
	httpReq, err := http.NewRequestWithContext(context.Background(), "POST",
		gastrologv1connect.VaultServiceListChunksProcedure,
		bytes.NewReader(reqBytes))
	if err != nil {
		t.Fatal(err)
	}
	httpReq.Header.Set("Content-Type", "application/proto")
	httpReq.Header.Set("Connect-Protocol-Version", "1")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httpReq)

	resp := rec.Result()
	defer resp.Body.Close()

	t.Logf("Status: %d", resp.StatusCode)
	t.Logf("Content-Type: %s", resp.Header.Get("Content-Type"))

	if resp.StatusCode != http.StatusOK {
		var body [4096]byte
		n, _ := resp.Body.Read(body[:])
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(body[:n]))
	}

	// Read the response body — should be raw proto bytes for unary Connect.
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}

	t.Logf("Response body length: %d bytes", buf.Len())

	// Unmarshal the response.
	respMsg := &apiv1.ListChunksResponse{}
	if err := proto.Unmarshal(buf.Bytes(), respMsg); err != nil {
		t.Fatalf("unmarshal response: %v (body hex: %x)", err, buf.Bytes())
	}

	if len(respMsg.GetChunks()) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(respMsg.GetChunks()))
	}
	for i, c := range respMsg.GetChunks() {
		t.Logf("chunk[%d]: id=%s records=%d", i, c.GetId(), c.GetRecordCount())
	}
}

// fakeVaultService implements just the ListChunks method for testing.
type fakeVaultService struct {
	gastrologv1connect.UnimplementedVaultServiceHandler
	chunks []*apiv1.ChunkMeta
}

func (f *fakeVaultService) ListChunks(
	ctx context.Context,
	req *connect.Request[apiv1.ListChunksRequest],
) (*connect.Response[apiv1.ListChunksResponse], error) {
	return connect.NewResponse(&apiv1.ListChunksResponse{Chunks: f.chunks}), nil
}
