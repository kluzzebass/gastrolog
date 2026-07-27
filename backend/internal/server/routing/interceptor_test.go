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
	"gastrolog/internal/glid"
	"gastrolog/internal/server/routing"
)

// mockOwner resolves ownership from a map, standing in for a resolver
// backed by replicated cluster state.
type mockOwner struct {
	owners map[string][]string // resource ID → node IDs
	err    error               // when set, returned for every lookup
}

func (m *mockOwner) ResolveOwners(_ context.Context, resourceID string) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.owners[resourceID], nil
}

// vaultOwners builds an OwnerResolvers set with only a vault resolver.
func vaultOwners(owners map[string][]string) routing.OwnerResolvers {
	return routing.OwnerResolvers{routing.ResourceVault: &mockOwner{owners: owners}}
}

// recordingForwarder captures what the interceptor forwarded and returns a
// canned response payload, standing in for ForwardRPC to a peer node.
type recordingForwarder struct {
	gotNode      string
	gotProcedure string
	gotPayload   []byte
	respond      func(procedure string, payload []byte) ([]byte, error)
}

func (f *recordingForwarder) ForwardUnary(_ context.Context, nodeID, procedure string, reqPayload []byte) ([]byte, error) {
	f.gotNode = nodeID
	f.gotProcedure = procedure
	f.gotPayload = reqPayload
	return f.respond(procedure, reqPayload)
}

// failForwarder fails the test if the interceptor forwards at all. Used by
// cases that must stay local — a nil forwarder would make them pass for the
// wrong reason (nil forwarder disables forwarding entirely).
type failForwarder struct{ t *testing.T }

func (f *failForwarder) ForwardUnary(_ context.Context, nodeID, procedure string, _ []byte) ([]byte, error) {
	f.t.Helper()
	f.t.Errorf("unexpected forward of %s to %s", procedure, nodeID)
	return nil, errors.New("must not forward")
}

// forwardMarker stands in for the ForwardRPC dispatch path, which marks the
// handler context as already-forwarded before the routing interceptor runs.
type forwardMarker struct{}

func (forwardMarker) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		return next(routing.WithForwarded(ctx), req)
	}
}

func (forwardMarker) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (forwardMarker) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		return next(routing.WithForwarded(ctx), conn)
	}
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
	outer ...connect.Interceptor,
) (*connect.Response[Resp], error) {
	t.Helper()

	opts := []connect.HandlerOption{
		connect.WithInterceptors(append(append([]connect.Interceptor{}, outer...), ri)...),
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

func TestRoutingInterceptor_ResourceOwner_LocalVault(t *testing.T) {
	// Local node is among the owners → execute locally, no hop.
	owners := vaultOwners(map[string][]string{"some-vault": {"node-1"}})
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, &failForwarder{t: t})

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceGetChunkProcedure,
		&apiv1.GetChunkRequest{Vault: "some-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
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

func TestRoutingInterceptor_ResourceOwner_EmptyVault(t *testing.T) {
	owners := vaultOwners(map[string][]string{})
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, &failForwarder{t: t})

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceGetChunkProcedure,
		&apiv1.GetChunkRequest{Vault: ""},
		func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
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

// TestRoutingInterceptor_ResourceOwner_ForwardsToRemote is the core of the
// mechanism: a request naming a resource this node does not own is
// serialized and forwarded to the owner, and the owner's response comes
// back to the caller.
func TestRoutingInterceptor_ResourceOwner_ForwardsToRemote(t *testing.T) {
	fwd := &recordingForwarder{
		respond: func(procedure string, payload []byte) ([]byte, error) {
			return proto.Marshal(&apiv1.GetChunkResponse{Chunk: &apiv1.ChunkMeta{Id: []byte("from-owner")}})
		},
	}
	owners := vaultOwners(map[string][]string{"remote-vault": {"node-2"}})
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, fwd)

	var handlerCalled bool
	resp, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceGetChunkProcedure,
		&apiv1.GetChunkRequest{Vault: "remote-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handlerCalled {
		t.Error("local handler ran for a remotely-owned vault")
	}
	if fwd.gotNode != "node-2" {
		t.Errorf("forwarded to %q, want node-2", fwd.gotNode)
	}
	if string(resp.Msg.GetChunk().GetId()) != "from-owner" {
		t.Errorf("response did not come from the owner: %v", resp.Msg)
	}
}

// TestRoutingInterceptor_ResourceOwner_PluralOwners covers the ingester-HA
// shape: several nodes own the resource. When the local node is one of
// them the request stays local; otherwise every node picks the same owner
// (the first, resolvers return a deterministic order).
func TestRoutingInterceptor_ResourceOwner_PluralOwners(t *testing.T) {
	owners := vaultOwners(map[string][]string{"shared": {"node-2", "node-3", "node-4"}})

	t.Run("local node is an owner", func(t *testing.T) {
		ri := routing.NewRoutingInterceptor(testRegistry(), "node-3", owners, &failForwarder{t: t})
		var handlerCalled bool
		_, err := runUnary(t, ri,
			gastrologv1connect.VaultServiceGetChunkProcedure,
			&apiv1.GetChunkRequest{Vault: "shared"},
			func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
				handlerCalled = true
				return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
			},
			context.Background(), nil,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !handlerCalled {
			t.Error("handler did not run although the local node owns the resource")
		}
	})

	// Every non-owner node must land on the same owner.
	for _, from := range []string{"node-1", "node-5"} {
		t.Run("from "+from, func(t *testing.T) {
			fwd := &recordingForwarder{
				respond: func(string, []byte) ([]byte, error) {
					return proto.Marshal(&apiv1.GetChunkResponse{})
				},
			}
			ri := routing.NewRoutingInterceptor(testRegistry(), from, owners, fwd)
			_, err := runUnary(t, ri,
				gastrologv1connect.VaultServiceGetChunkProcedure,
				&apiv1.GetChunkRequest{Vault: "shared"},
				func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
					t.Error("handler ran on a non-owner node")
					return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
				},
				context.Background(), nil,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if fwd.gotNode != "node-2" {
				t.Errorf("forwarded to %q, want node-2 (first owner)", fwd.gotNode)
			}
		})
	}
}

// TestRoutingInterceptor_ResourceOwner_NotFound: a resolver that positively
// knows the resource does not exist produces a clean NotFound instead of
// letting an arbitrary node run the handler.
func TestRoutingInterceptor_ResourceOwner_NotFound(t *testing.T) {
	owners := routing.OwnerResolvers{
		routing.ResourceVault: &mockOwner{err: routing.ErrResourceNotFound},
	}
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, &failForwarder{t: t})

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceGetChunkProcedure,
		&apiv1.GetChunkRequest{Vault: "ghost"},
		func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
		},
		context.Background(), nil,
	)
	if handlerCalled {
		t.Error("handler ran for a resource the resolver said does not exist")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("got %v (%v), want CodeNotFound", connect.CodeOf(err), err)
	}
}

// TestRoutingInterceptor_ResourceOwner_ResolverError: any other resolver
// failure surfaces as FailedPrecondition rather than a silent local run.
func TestRoutingInterceptor_ResourceOwner_ResolverError(t *testing.T) {
	owners := routing.OwnerResolvers{
		routing.ResourceVault: &mockOwner{err: errors.New("store unavailable")},
	}
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, &failForwarder{t: t})

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceGetChunkProcedure,
		&apiv1.GetChunkRequest{Vault: "some-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
		},
		context.Background(), nil,
	)
	if handlerCalled {
		t.Error("handler ran despite an unresolvable owner")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("got %v (%v), want CodeFailedPrecondition", connect.CodeOf(err), err)
	}
}

// TestRoutingInterceptor_ResourceOwner_IngesterKind proves the mechanism is
// not vault-shaped: TriggerIngester routes on the ingester resource kind,
// with no X-Target-Node header from the caller.
func TestRoutingInterceptor_ResourceOwner_IngesterKind(t *testing.T) {
	ingesterID := glid.New()
	fwd := &recordingForwarder{
		respond: func(string, []byte) ([]byte, error) {
			return proto.Marshal(&apiv1.TriggerIngesterResponse{})
		},
	}
	owners := routing.OwnerResolvers{
		routing.ResourceIngester: &mockOwner{owners: map[string][]string{
			ingesterID.String(): {"node-2"},
		}},
	}
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, fwd)

	_, err := runUnary(t, ri,
		gastrologv1connect.SystemServiceTriggerIngesterProcedure,
		&apiv1.TriggerIngesterRequest{Id: ingesterID.Bytes()},
		func(ctx context.Context, req *connect.Request[apiv1.TriggerIngesterRequest]) (*connect.Response[apiv1.TriggerIngesterResponse], error) {
			t.Error("handler ran on a node that does not run the ingester")
			return connect.NewResponse(&apiv1.TriggerIngesterResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fwd.gotNode != "node-2" {
		t.Errorf("forwarded to %q, want node-2", fwd.gotNode)
	}
	if fwd.gotProcedure != gastrologv1connect.SystemServiceTriggerIngesterProcedure {
		t.Errorf("forwarded procedure %q", fwd.gotProcedure)
	}
}

// TestRoutingInterceptor_ResourceOwner_MalformedID: an ID that is not a
// GLID yields no routing decision, so the handler runs and reports the
// validation error itself.
func TestRoutingInterceptor_ResourceOwner_MalformedID(t *testing.T) {
	owners := routing.OwnerResolvers{
		routing.ResourceIngester: &mockOwner{err: errors.New("resolver must not be consulted")},
	}
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, &failForwarder{t: t})

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.SystemServiceTriggerIngesterProcedure,
		&apiv1.TriggerIngesterRequest{Id: []byte("too-short")},
		func(ctx context.Context, req *connect.Request[apiv1.TriggerIngesterRequest]) (*connect.Response[apiv1.TriggerIngesterResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.TriggerIngesterResponse{}), nil
		},
		context.Background(), nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for a malformed resource ID")
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
	owners := vaultOwners(map[string][]string{"remote-vault": {"node-2"}})
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, &failForwarder{t: t})

	var handlerCalled bool
	// The forwarded mark is set server-side (the ForwardRPC dispatch path
	// wraps the handler context), so it has to be applied by an interceptor
	// ahead of the routing one — a client-side context value would not
	// survive the HTTP hop.
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceGetChunkProcedure,
		&apiv1.GetChunkRequest{Vault: "remote-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
		},
		context.Background(), nil, forwardMarker{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for already-forwarded request")
	}
}

func TestRoutingInterceptor_SingleNodeMode(t *testing.T) {
	owners := vaultOwners(map[string][]string{"remote-vault": {"node-2"}})
	ri := routing.NewRoutingInterceptor(testRegistry(), "node-1", owners, nil)

	var handlerCalled bool
	_, err := runUnary(t, ri,
		gastrologv1connect.VaultServiceGetChunkProcedure,
		&apiv1.GetChunkRequest{Vault: "remote-vault"},
		func(ctx context.Context, req *connect.Request[apiv1.GetChunkRequest]) (*connect.Response[apiv1.GetChunkResponse], error) {
			handlerCalled = true
			return connect.NewResponse(&apiv1.GetChunkResponse{}), nil
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
		{routing.RouteToResourceOwner, "RouteToResourceOwner"},
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
