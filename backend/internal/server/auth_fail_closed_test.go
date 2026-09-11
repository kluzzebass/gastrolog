package server_test

// A server built with no way to tell callers apart must refuse them all.
// The alternative is what this replaces: a construction path that omitted
// the token service served the entire API to anyone who reached the
// listener, and the only sign was the absence of a rejection.

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"
)

func newFailClosedTestServer(t *testing.T, cfg server.Config) *http.Client {
	t.Helper()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	srv := server.New(orch, cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil, cfg)
	return &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
}

func TestServerWithNoAuthenticatorRefusesEveryRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	httpClient := newFailClosedTestServer(t, server.Config{})

	system := gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")
	_, err := system.GetSystem(ctx, connect.NewRequest(&gastrologv1.GetSystemRequest{}))
	if err == nil {
		t.Fatal("a server with no authenticator served a unary RPC")
	}
	if got := connect.CodeOf(err); got != connect.CodeUnauthenticated {
		t.Fatalf("unary refused with %v, want %v (%v)", got, connect.CodeUnauthenticated, err)
	}

	// Claims the caller puts on the context itself must not get in either:
	// without TrustContextClaims there is nothing that vouched for them.
	if _, err := system.GetSystem(adminContext(ctx),
		connect.NewRequest(&gastrologv1.GetSystemRequest{})); err == nil {
		t.Fatal("a server with no authenticator honoured caller-supplied claims")
	}

	// Streaming handlers go through a different wrapper and must refuse too.
	query := gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")
	stream, err := query.Search(ctx, connect.NewRequest(&gastrologv1.SearchRequest{}))
	if err == nil {
		_, err = stream.Receive(), error(nil)
		if rerr := stream.Err(); rerr != nil {
			err = rerr
		}
	}
	if err == nil {
		t.Fatal("a server with no authenticator served a streaming RPC")
	}
	if got := connect.CodeOf(err); got != connect.CodeUnauthenticated {
		t.Fatalf("stream refused with %v, want %v (%v)", got, connect.CodeUnauthenticated, err)
	}
}

// The two ways of saying "this listener is not authenticating" still work,
// so the refusal above is about the unstated case, not about turning auth
// into a hard requirement everywhere.
func TestStatedUnauthenticatedModesStillServe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	noAuth := gastrologv1connect.NewSystemServiceClient(
		newFailClosedTestServer(t, server.Config{NoAuth: true}), "http://embedded")
	if _, err := noAuth.GetSystem(ctx, connect.NewRequest(&gastrologv1.GetSystemRequest{})); err != nil {
		t.Fatalf("NoAuth server refused a request: %v", err)
	}

	trusting := gastrologv1connect.NewSystemServiceClient(
		newFailClosedTestServer(t, server.Config{TrustContextClaims: true}), "http://embedded")
	if _, err := trusting.GetSystem(adminContext(ctx), connect.NewRequest(&gastrologv1.GetSystemRequest{})); err != nil {
		t.Fatalf("TrustContextClaims server refused a request: %v", err)
	}
}
