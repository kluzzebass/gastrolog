// Package routing provides the cluster routing layer for GastroLog.
//
// Every Connect RPC is classified with a routing strategy that determines
// how it behaves in a multi-node cluster:
//
//   - RouteLocal: execute on whichever node received the request
//   - RouteLeader: Raft Apply handles leader-forwarding (no interceptor action)
//   - RouteTargeted: route to the node that owns the vault (via ForwardRPC)
//   - RouteFanOut: handler manages its own fan-out to all nodes
//
// The routing interceptor uses this classification to auto-forward
// RouteTargeted RPCs and explicit node-targeted requests.
package routing

import (
	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// Strategy classifies how an RPC is routed in a multi-node cluster.
type Strategy int

const (
	// RouteLocal executes on whichever node received the request.
	// No forwarding needed — all nodes can serve these RPCs identically.
	RouteLocal Strategy = iota + 1

	// RouteLeader defers to the Raft leader. The config store's Apply
	// mechanism already handles leader-forwarding, so the routing
	// interceptor does not need to act.
	RouteLeader

	// RouteTargeted routes to the node that owns the vault referenced
	// in the request. The routing interceptor resolves the vault owner
	// and forwards via ForwardRPC if the vault is on a remote node.
	RouteTargeted

	// RouteFanOut is handled by the handler itself, which fans out to
	// all nodes, merges results, and streams them back. The interceptor
	// passes these through without action.
	RouteFanOut
)

// String returns a human-readable name for the strategy.
func (s Strategy) String() string {
	switch s {
	case RouteLocal:
		return "RouteLocal"
	case RouteLeader:
		return "RouteLeader"
	case RouteTargeted:
		return "RouteTargeted"
	case RouteFanOut:
		return "RouteFanOut"
	default:
		return "Unknown"
	}
}

// RPCRoute describes the routing behavior for a single RPC procedure.
type RPCRoute struct {
	// Strategy is the routing classification for this RPC.
	Strategy Strategy

	// IsStreaming is true for server-streaming RPCs. The interceptor uses
	// this to choose the correct forwarding path (unary vs streaming bridge).
	IsStreaming bool

	// WrapResponse deserializes raw proto bytes into a connect.AnyResponse
	// of the correct type. Only set for RouteTargeted unary RPCs that the
	// interceptor may forward. Nil for all other RPCs.
	WrapResponse func([]byte) (connect.AnyResponse, error)
}

// NewRespWrapper returns a WrapResponse function for a given proto response
// type. Uses generics to call connect.NewResponse with the correct type
// parameter, sidestepping Connect's sealed AnyResponse interface.
func NewRespWrapper[T any, PT interface {
	*T
	proto.Message
}]() func([]byte) (connect.AnyResponse, error) {
	return func(data []byte) (connect.AnyResponse, error) {
		msg := PT(new(T))
		if err := proto.Unmarshal(data, msg); err != nil {
			return nil, err
		}
		return connect.NewResponse(msg), nil
	}
}

// Registry maps Connect procedure strings to their routing metadata.
// Built once at startup from DefaultRoutes() and never mutated after.
type Registry struct {
	routes map[string]RPCRoute
}

// NewRegistry creates a Registry from a slice of procedure→route pairs.
func NewRegistry(routes map[string]RPCRoute) *Registry {
	return &Registry{routes: routes}
}

// Lookup returns the route for a procedure. The second return value is false
// if the procedure is not registered.
func (r *Registry) Lookup(procedure string) (RPCRoute, bool) {
	route, ok := r.routes[procedure]
	return route, ok
}

// Procedures returns all registered procedure names (for testing).
func (r *Registry) Procedures() []string {
	procs := make([]string, 0, len(r.routes))
	for p := range r.routes {
		procs = append(procs, p)
	}
	return procs
}

// Len returns the number of registered procedures.
func (r *Registry) Len() int {
	return len(r.routes)
}
