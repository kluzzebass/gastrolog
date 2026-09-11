package server_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// recordingForwarder captures every routed request and answers a valid,
// empty vault validation so the caller sees a routed success.
type recordingForwarder struct {
	mu    sync.Mutex
	calls []string // "target procedure"
}

func (f *recordingForwarder) ForwardUnary(_ context.Context, nodeID, procedure string, _ []byte) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, nodeID+" "+procedure)
	f.mu.Unlock()
	return proto.Marshal(&gastrologv1.ValidateVaultResponse{Valid: true})
}

func (f *recordingForwarder) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// serverWithRemoteVault builds a node that knows a vault placed on another
// node, with a recording routing forwarder.
func serverWithRemoteVault(t *testing.T) (*server.Server, *recordingForwarder, glid.GLID) {
	t.Helper()
	ctx := context.Background()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: "node-local", SystemLoader: cfgStore})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = orch.Stop() })
	vaultID := glid.New()
	if err := cfgStore.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "elsewhere", Type: system.VaultTypeMemory}); err != nil {
		t.Fatal(err)
	}
	if err := cfgStore.SetVaultPlacements(ctx, vaultID, []system.VaultPlacement{{StorageID: system.SyntheticStorageID("node-remote"), Leader: true}}); err != nil {
		t.Fatal(err)
	}
	fwd := &recordingForwarder{}
	srv := server.New(orch, cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil, server.Config{
		NoAuth: true, NodeID: "node-local", RoutingForwarder: fwd})
	return srv, fwd, vaultID
}

// The CLI's socket is a first-hop client channel: an owner-routed request
// for a vault held elsewhere must be forwarded to that node, exactly as it
// is over TCP, rather than answered locally by a node without the vault.
func TestUnixSocketRoutesOwnerScopedRequestsToTheHoldingNode(t *testing.T) {
	srv, fwd, vaultID := serverWithRemoteVault(t)
	dir, err := os.MkdirTemp("", "glsock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	if err := srv.ListenUnix(sock); err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
	client := gastrologv1connect.NewVaultServiceClient(httpClient, "http://unix")
	resp, err := client.ValidateVault(context.Background(), connect.NewRequest(&gastrologv1.ValidateVaultRequest{Vault: vaultID.String()}))
	if err != nil {
		t.Fatalf("validate over the socket: %v", err)
	}
	if !resp.Msg.GetValid() {
		t.Fatalf("routed response not returned to the socket caller: %v", resp.Msg)
	}
	calls := fwd.snapshot()
	if len(calls) != 1 || calls[0] != "node-remote "+gastrologv1connect.VaultServiceValidateVaultProcedure {
		t.Fatalf("socket request was not routed to the vault's node; forwarder calls = %v", calls)
	}
}

// The internal handler serves requests a peer already forwarded; routing
// there would loop, so it must stay unrouted.
func TestInternalHandlerDoesNotRouteForwardedRequests(t *testing.T) {
	srv, fwd, vaultID := serverWithRemoteVault(t)
	ts := httptest.NewServer(srv.BuildInternalHandler())
	t.Cleanup(ts.Close)

	client := gastrologv1connect.NewVaultServiceClient(ts.Client(), ts.URL)
	_, _ = client.ValidateVault(context.Background(), connect.NewRequest(&gastrologv1.ValidateVaultRequest{Vault: vaultID.String()}))
	if calls := fwd.snapshot(); len(calls) != 0 {
		t.Fatalf("internal handler routed a forwarded request onward: %v", calls)
	}
}
