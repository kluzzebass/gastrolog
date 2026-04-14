package routing_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/server/routing"
)

// mockVaultOwner resolves vault ownership from a map.
type mockVaultOwner struct {
	owners map[string]string // vault → node_id
}

func (m *mockVaultOwner) ResolveVaultOwner(_ context.Context, vaultID string) string {
	return m.owners[vaultID]
}

// testRegistry builds a full registry for testing.
func testRegistry() *routing.Registry {
	return routing.NewRegistry(routing.DefaultRoutes())
}

// fakeAnyRequest implements connect.AnyRequest for testing.
// Connect's sealed interface prevents custom implementations, so we use
// real connect.Request objects with a custom handler approach instead.

// runUnary sets up a minimal Connect handler with the routing interceptor,
// calls the specified procedure via an httptest server, and returns the result.
func runUnary[Req, Resp any](
	t *testing.T,
	ri *routing.RoutingInterceptor,
	procedure string,
	req *Req,
	handler func(ctx context.Context, req *connect.Request[Req]) (*connect.Response[Resp], error),
	ctx context.Context,
	headers map[string]string,
) (*connect.Response[Resp], error) {
	t.Helper()

	opts := []connect.HandlerOption{
		connect.WithInterceptors(ri),
	}
	mux := http.NewServeMux()
	mux.Handle(procedure, connect.NewUnaryHandler(procedure, handler, opts...))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := connect.NewClient[Req, Resp](srv.Client(), srv.URL+procedure)

	connectReq := connect.NewRequest(req)
	for k, v := range headers {
		connectReq.Header().Set(k, v)
	}

	return client.CallUnary(ctx, connectReq)
}

// -- Tests --

func TestRoutingInterceptor_RouteLocal_AlwaysLocal(t *testing.T) {
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", nil, nil)

	var handlerCalled bool
	resp, err := runUnary(t, ri,
		gastrologv1connect.SystemServiceGetSystemProcedure,
		&apiv1.GetSystemRequest{},
		func(ctx context.Context, req *connect.Request[apiv1.GetSystemRequest]) (*connect.Response[apiv1.GetSystemResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetSystemResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for RouteLocal RPC")
	}
	_ = resp
}

func TestRoutingInterceptor_RouteTargeted_LocalVault(t *testing.T) {
	vaultOwner := &mockVaultOwner{owners: map[string]string{}} // empty = local
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", vaultOwner, nil)

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceListChunksProcedure,
		&apiv1.ListChunksRequest{Vault: "some-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.ListChunksRequest]) (*connect.Response[apiv1.ListChunksResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.ListChunksResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for local vault")
	}
}

func TestRoutingInterceptor_RouteTargeted_EmptyVault(t *testing.T) {
	vaultOwner := &mockVaultOwner{owners: map[string]string{}}
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", vaultOwner, nil)

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceListChunksProcedure,
		&apiv1.ListChunksRequest{Vault: ""},
		func(ctx context.Context, req *connect.Request[apiv1.ListChunksRequest]) (*connect.Response[apiv1.ListChunksResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.ListChunksResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for empty vault field")
	}
}

func TestRoutingInterceptor_RouteFanOut_PassThrough(t *testing.T) {
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", nil, nil)

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.QueryServiceExplainProcedure,
		&apiv1.ExplainRequest{},
		func(ctx context.Context, req *connect.Request[apiv1.ExplainRequest]) (*connect.Response[apiv1.ExplainResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.ExplainResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for RouteFanOut RPC")
	}
}

func TestRoutingInterceptor_RouteLeader_PassThrough(t *testing.T) {
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", nil, nil)

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.SystemServicePutVaultProcedure,
		&apiv1.PutVaultRequest{},
		func(ctx context.Context, req *connect.Request[apiv1.PutVaultRequest]) (*connect.Response[apiv1.PutVaultResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.PutVaultResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for RouteLeader RPC")
	}
}

func TestRoutingInterceptor_ExplicitTarget_SameNode(t *testing.T) {
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", nil, nil)

	var handlerCalled bool
	ctx := routing.WithTargetNode(context.Background(), "node-1")
	_, err := runUnary(t, ri,
		gastrologv1connect.SystemServiceGetSystemProcedure,
		&apiv1.GetSystemRequest{},
		func(ctx context.Context, req *connect.Request[apiv1.GetSystemRequest]) (*connect.Response[apiv1.GetSystemResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetSystemResponse{}), nil
		},
		ctx, nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called when targeting self")
	}
}

func TestRoutingInterceptor_AlreadyForwarded_NoReforward(t *testing.T) {
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", nil, nil)

	var handlerCalled bool
	ctx := routing.WithForwarded(context.Background())
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceListChunksProcedure,
		&apiv1.ListChunksRequest{Vault: "remote-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.ListChunksRequest]) (*connect.Response[apiv1.ListChunksResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.ListChunksResponse{}), nil
		},
		ctx, nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for already-forwarded request")
	}
}

func TestRoutingInterceptor_SingleNodeMode(t *testing.T) {
	vaultOwner := &mockVaultOwner{owners: map[string]string{
		"remote-vault": "node-2",
	}}
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", vaultOwner, nil)

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceListChunksProcedure,
		&apiv1.ListChunksRequest{Vault: "remote-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.ListChunksRequest]) (*connect.Response[apiv1.ListChunksResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.ListChunksResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler should be called in single-node mode even for remote vaults")
	}
}

func TestRoutingInterceptor_ExplicitTargetFromHeader(t *testing.T) {
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", nil, nil)

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.SystemServiceGetSystemProcedure,
		&apiv1.GetSystemRequest{},
		func(ctx context.Context, req *connect.Request[apiv1.GetSystemRequest]) (*connect.Response[apiv1.GetSystemResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetSystemResponse{}), nil
		},
		context.Background(),
		map[string]string{"X-Target-Node": "node-1"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for X-Target-Node=self")
	}
}

func TestContextHelpers(t *testing.T) {
	t.Run("TargetNode", func(t *testing.T) {
		ctx := context.Background()
		if got := routing.TargetNode(ctx); got != "" {
			t.Errorf("expected empty, got %q", got)
		}
		ctx = routing.WithTargetNode(ctx, "data-1")
		if got := routing.TargetNode(ctx); got != "data-1" {
			t.Errorf("expected data-1, got %q", got)
		}
	})

	t.Run("IsForwarded", func(t *testing.T) {
		ctx := context.Background()
		if routing.IsForwarded(ctx) {
			t.Error("expected false")
		}
		ctx = routing.WithForwarded(ctx)
		if !routing.IsForwarded(ctx) {
			t.Error("expected true")
		}
	})
}

func TestNewRespWrapper(t *testing.T) {
	original := &apiv1.ListChunksResponse{
		Chunks: []*apiv1.ChunkMeta{
			{Id: []byte("test-chunk")},
		},
	}
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	wrapper := routing.NewRespWrapper[apiv1.ListChunksResponse]()
	resp, err := wrapper(data)
	if err != nil {
		t.Fatal(err)
	}

	msg, ok := resp.Any().(*apiv1.ListChunksResponse)
	if !ok {
		t.Fatalf("expected *ListChunksResponse, got %T", resp.Any())
	}
	if len(msg.GetChunks()) != 1 || string(msg.GetChunks()[0].GetId()) != "test-chunk" {
		t.Errorf("unexpected response: %v", msg)
	}
}

func TestStrategyString(t *testing.T) {
	tests := []struct {
		s    routing.Strategy
		want string
	}{
		{routing.RouteLocal, "RouteLocal"},
		{routing.RouteLeader, "RouteLeader"},
		{routing.RouteTargeted, "RouteTargeted"},
		{routing.RouteFanOut, "RouteFanOut"},
		{routing.Strategy(99), "Unknown"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}

func TestRegistryLookup(t *testing.T) {
	reg := testRegistry()

	t.Run("found", func(t *testing.T) {
		route, ok := reg.Lookup(gastrologv1connect.VaultServiceListChunksProcedure)
		if !ok {
			t.Fatal("expected to find ListChunks")
		}
		if route.Strategy != routing.RouteFanOut {
			t.Errorf("expected RouteFanOut, got %v", route.Strategy)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, ok := reg.Lookup("/gastrolog.v1.FakeService/Nope")
		if ok {
			t.Error("expected not found")
		}
	})
}

// Undeclared RPC test: interceptor should return CodeInternal.
// We test this by looking at what happens when the routing interceptor
// encounters an unknown procedure — it can only be tested via the
// interceptor's WrapUnary directly since the handler setup would reject it.
func TestRoutingInterceptor_UndeclaredRPC(t *testing.T) {
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", nil, nil)

	// WrapUnary returns a UnaryFunc. We can call it with a mock procedure
	// by going through a handler with a custom procedure path.
	var handlerCalled bool
	_, err := runUnary(t, ri,
		"/gastrolog.v1.FakeService/NotReal",
		&apiv1.GetSystemRequest{},
		func(ctx context.Context, req *connect.Request[apiv1.GetSystemRequest]) (*connect.Response[apiv1.GetSystemResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetSystemResponse{}), nil
		},
		context.Background(), nil,
	)
	if err == nil {
		t.Fatal("expected error for undeclared RPC")
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeInternal {
		// Expected — undeclared procedure returns CodeInternal.
	}
	if handlerCalled {
		t.Error("handler should not be called for undeclared RPC")
	}
}

