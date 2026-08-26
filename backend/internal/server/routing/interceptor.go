package routing

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// RoutingInterceptor is a Connect interceptor that auto-routes requests
// based on their strategy classification:
//
//   - RouteToResourceOwner: read the resource ID declared by the route,
//     resolve the owning node(s) from replicated state, forward via
//     ForwardRPC if the owner is remote.
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
	owners      OwnerResolvers
	forwarder   UnaryForwarder
}

// NewRoutingInterceptor creates a routing interceptor. If forwarder is nil
// (single-node mode), the interceptor is a no-op pass-through.
func NewRoutingInterceptor(registry *Registry, localNodeID string, owners OwnerResolvers, forwarder UnaryForwarder) *RoutingInterceptor {
	return &RoutingInterceptor{
		registry:    registry,
		localNodeID: localNodeID,
		owners:      owners,
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
		if route.Strategy == RouteToResourceOwner {
			target, err := ri.resolveOwner(ctx, route, req.Any())
			if err != nil {
				return nil, err
			}
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
		// concrete type. Server-streaming RouteToResourceOwner (ExportVault) and
		// RouteFanOut (Search, Follow) use handler-level routing.
		_ = route
		return next(ctx, conn)
	}
}

// WrapStreamingClient is a no-op for server-side interceptors.
func (ri *RoutingInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// resolveOwner reads the resource ID declared by the route, asks that
// resource kind's OwnerResolver which node(s) own it, and picks the node to
// execute on. An empty target means "execute locally".
//
// Selection over a plural owner set: prefer the local node when it is an
// owner (no hop), otherwise take the first — resolvers return a
// deterministic order, so every node in the cluster picks the same one.
func (ri *RoutingInterceptor) resolveOwner(ctx context.Context, route RPCRoute, msg any) (string, *connect.Error) {
	ref := route.Resource
	if ref == nil {
		return "", nil
	}
	resolver := ri.owners[ref.Kind]
	if resolver == nil {
		return "", nil
	}
	resourceID := ref.ID(msg)
	if resourceID == "" {
		return "", nil
	}

	owners, err := resolver.ResolveOwners(ctx, resourceID)
	if err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return "", connect.NewError(connect.CodeNotFound,
				fmt.Errorf("%s %s: %w", ref.Kind, resourceID, err))
		}
		return "", connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("resolve %s owner for %s: %w", ref.Kind, resourceID, err))
	}
	if len(owners) == 0 {
		return "", nil
	}
	if slices.Contains(owners, ri.localNodeID) {
		return ri.localNodeID, nil
	}
	return owners[0], nil
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
		if re, ok := errors.AsType[*RemoteError](fwdErr); ok {
			return nil, connect.NewError(connect.Code(re.Code), fmt.Errorf("%s", re.Message))
		}
		return nil, connect.NewError(connect.CodeUnavailable, fwdErr)
	}

	return route.WrapResponse(respPayload)
}

// Ensure RoutingInterceptor implements connect.Interceptor.
var _ connect.Interceptor = (*RoutingInterceptor)(nil)
