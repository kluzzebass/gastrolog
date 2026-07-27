// Package routing provides the cluster routing layer for GastroLog.
//
// Every Connect RPC is classified with a routing strategy that determines
// how it behaves in a multi-node cluster:
//
//   - RouteLocal: execute on whichever node received the request
//   - RouteLeader: Raft Apply handles leader-forwarding (no interceptor action)
//   - RouteToResourceOwner: route to the node that owns the resource named in
//     the request (via ForwardRPC) — no Raft round-trip
//   - RouteFanOut: handler manages its own fan-out to all nodes
//
// The routing interceptor uses this classification to auto-forward
// RouteToResourceOwner RPCs and explicit node-targeted requests.
package routing

import (
	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"gastrolog/internal/glid"
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

	// RouteToResourceOwner routes to the node that owns the resource
	// referenced in the request — the vault a chunk lives in, the node
	// running an ingester, and so on. The route declares which resource
	// via RPCRoute.Resource; the interceptor resolves the owning node(s)
	// from replicated cluster state and forwards via ForwardRPC when the
	// owner is remote.
	//
	// This is the strategy for imperative node actions (seal, reindex,
	// trigger): they reach the right node WITHOUT being tunnelled through
	// a config mutation and a Raft round-trip, which is what RouteLeader
	// is for.
	RouteToResourceOwner

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
	case RouteToResourceOwner:
		return "RouteToResourceOwner"
	case RouteFanOut:
		return "RouteFanOut"
	default:
		return "Unknown"
	}
}

// ResourceKind names a class of cluster resource that an RPC can target.
// Each kind has exactly one registered OwnerResolver (see OwnerResolvers)
// that answers "which node(s) own this resource" from replicated state.
type ResourceKind string

const (
	// ResourceVault is a vault; its owner is the vault leader node.
	ResourceVault ResourceKind = "vault"

	// ResourceIngester is a configured ingester; its owners are the nodes
	// currently running it (one node, or several under ingester HA).
	ResourceIngester ResourceKind = "ingester"
)

// ResourceRef declares which resource a RouteToResourceOwner RPC targets
// and how to read that resource's ID out of the request message.
//
// The extraction is declared per-procedure (via OwnerOf) rather than
// duck-typed at runtime: proto messages carry many differently-scoped
// `id` fields, and guessing between them silently misroutes requests.
type ResourceRef struct {
	// Kind selects the OwnerResolver.
	Kind ResourceKind

	// ID returns the resource ID from the request message, or "" when the
	// request does not name one (empty field, wrong message type, malformed
	// ID). "" means "do not route" — the request executes locally and the
	// handler produces its own validation error.
	ID func(msg any) string
}

// OwnerOf builds a ResourceRef for a concrete request type. Written as a
// method expression at the call site, so the compiler checks the getter
// belongs to the message the procedure actually carries:
//
//	OwnerOf(ResourceVault, (*apiv1.SealVaultRequest).GetVault)
func OwnerOf[T proto.Message](kind ResourceKind, extract func(T) string) *ResourceRef {
	return &ResourceRef{
		Kind: kind,
		ID: func(msg any) string {
			m, ok := msg.(T)
			if !ok {
				return ""
			}
			return extract(m)
		},
	}
}

// ProtoGLID renders a raw proto ID field (16 bytes) as its canonical GLID
// string. Returns "" for any other length so a malformed ID falls through
// to the handler, which reports InvalidArgument.
func ProtoGLID(b []byte) string {
	if len(b) != glid.Size {
		return ""
	}
	return glid.FromBytes(b).String()
}

// RPCRoute describes the routing behavior for a single RPC procedure.
type RPCRoute struct {
	// Strategy is the routing classification for this RPC.
	Strategy Strategy

	// Resource declares the resource this RPC targets. Required for
	// RouteToResourceOwner (enforced by TestRouteToResourceOwnerHaveResource),
	// nil for every other strategy.
	Resource *ResourceRef

	// IsStreaming is true for server-streaming RPCs. The interceptor uses
	// this to choose the correct forwarding path (unary vs streaming bridge).
	IsStreaming bool

	// WrapResponse deserializes raw proto bytes into a connect.AnyResponse
	// of the correct type. Only set for RouteToResourceOwner unary RPCs
	// that the interceptor may forward. Nil for all other RPCs.
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
