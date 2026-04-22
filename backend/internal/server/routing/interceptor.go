package routing

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// RoutingInterceptor is a Connect interceptor that auto-routes requests
// based on their strategy classification:
//
//   - RouteTargeted: duck-type GetVaultId() on the request, resolve the
//     owning node, forward via ForwardRPC if remote.
//   - Explicit targeting: honor routing.WithTargetNode(ctx, nodeID).
//   - Already forwarded: execute locally (loop prevention).
//   - Everything else (RouteLocal, RouteLeader, RouteFanOut): pass through.
//
// UnaryForwarder sends serialized requests to remote nodes.
type UnaryForwarder interface {
	ForwardUnary(ctx context.Context, nodeID, procedure string, reqPayload []byte) ([]byte, error)
}

type RoutingInterceptor struct {
	registry    *Registry
	localNodeID string
	vaultOwner  VaultOwnerResolver
	forwarder   UnaryForwarder
}

// NewRoutingInterceptor creates a routing interceptor. If forwarder is nil
// (single-node mode), the interceptor is a no-op pass-through.
func NewRoutingInterceptor(registry *Registry, localNodeID string, vaultOwner VaultOwnerResolver, forwarder UnaryForwarder) *RoutingInterceptor {
	return &RoutingInterceptor{
		registry:    registry,
		localNodeID: localNodeID,
		vaultOwner:  vaultOwner,
		forwarder:   forwarder,
	}
}

// WrapUnary implements connect.Interceptor for unary RPCs.
func (ri *RoutingInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		procedure := req.Spec().Procedure

		// Already forwarded — execute locally (loop prevention).
		if IsForwarded(ctx) {
			return next(ctx, req)
		}

		// Read X-Target-Node from request headers into context.
		if target := req.Header().Get("X-Target-Node"); target != "" {
			ctx = WithTargetNode(ctx, target)
		}

		// Look up the route for this procedure.
		route, ok := ri.registry.Lookup(procedure)
		if !ok {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("undeclared procedure: %s", procedure))
		}

		// Check explicit targeting first.
		if target := TargetNode(ctx); target != "" {
			if target == ri.localNodeID || ri.forwarder == nil {
				return next(ctx, req)
			}
			return ri.forwardUnary(ctx, target, procedure, route, req)
		}

		// Strategy-based routing.
		if route.Strategy == RouteTargeted {
			target := ri.resolveVaultTarget(ctx, req.Any())
			if target == "" || target == ri.localNodeID || ri.forwarder == nil {
				return next(ctx, req)
			}
			return ri.forwardUnary(ctx, target, procedure, route, req)
		}

		// RouteLocal, RouteLeader, RouteFanOut — pass through.
		return next(ctx, req)
	}
}

// WrapStreamingHandler implements connect.Interceptor for server-side streaming RPCs.
func (ri *RoutingInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		procedure := conn.Spec().Procedure

		// Already forwarded — execute locally.
		if IsForwarded(ctx) {
			return next(ctx, conn)
		}

		// Read X-Target-Node from request headers into context.
		if target := conn.RequestHeader().Get("X-Target-Node"); target != "" {
			ctx = WithTargetNode(ctx, target)
		}

		route, ok := ri.registry.Lookup(procedure)
		if !ok {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("undeclared procedure: %s", procedure))
		}

		// Streaming RPCs pass through to the handler which manages its own
		// routing. The interceptor can't generically forward streaming RPCs
		// because Connect's StreamingHandlerConn.Receive() requires a
		// concrete type. Server-streaming RouteTargeted (ExportVault) and
		// RouteFanOut (Search, Follow) use handler-level routing.
		_ = route
		return next(ctx, conn)
	}
}

// WrapStreamingClient is a no-op for server-side interceptors.
func (ri *RoutingInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// vaultGetter is the duck-typed interface for proto messages with a vault field.
// All RouteTargeted request messages (ListChunksRequest, GetChunkRequest, etc.)
// have a `string vault = 1` field that generates this getter.
type vaultGetter interface {
	GetVault() string
}

// resolveVaultTarget extracts the vault ID from the request (duck-typed)
// and resolves which node owns it.
func (ri *RoutingInterceptor) resolveVaultTarget(ctx context.Context, msg any) string {
	if ri.vaultOwner == nil {
		return ""
	}
	v, ok := msg.(vaultGetter)
	if !ok || v.GetVault() == "" {
		return ""
	}
	return ri.vaultOwner.ResolveVaultOwner(ctx, v.GetVault())
}

// forwardUnary serializes the request, forwards via ForwardRPC, and returns
// a Connect response wrapping the deserialized response proto.
func (ri *RoutingInterceptor) forwardUnary(ctx context.Context, target, procedure string, route RPCRoute, req connect.AnyRequest) (connect.AnyResponse, error) {
	if route.WrapResponse == nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no WrapResponse for %s — cannot forward", procedure))
	}

	protoMsg, ok := req.Any().(proto.Message)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, errors.New("request is not a proto.Message"))
	}
	payload, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("marshal request: %w", err))
	}

	respPayload, fwdErr := ri.forwarder.ForwardUnary(ctx, target, procedure, payload)
	if fwdErr != nil {
		var re *RemoteError
		if errors.As(fwdErr, &re) {
			return nil, connect.NewError(connect.Code(re.Code), fmt.Errorf("%s", re.Message))
		}
		return nil, connect.NewError(connect.CodeUnavailable, fwdErr)
	}

	return route.WrapResponse(respPayload)
}

// Ensure RoutingInterceptor implements connect.Interceptor.
var _ connect.Interceptor = (*RoutingInterceptor)(nil)
