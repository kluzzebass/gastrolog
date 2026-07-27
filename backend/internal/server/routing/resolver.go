package routing

import (
	"context"
	"errors"
)

// ErrResourceNotFound is returned by an OwnerResolver when replicated state
// positively says the resource does not exist. The interceptor turns it into
// connect.CodeNotFound instead of letting an arbitrary node run the handler
// and report a locally-flavored error.
var ErrResourceNotFound = errors.New("resource not found")

// OwnerResolver answers "which node(s) own this resource?" for one
// ResourceKind. Implementations MUST read replicated cluster state (the
// cluster-ctl Raft config/runtime), never node-local knowledge: every node
// has to give the same answer, or a RouteToResourceOwner RPC lands
// differently depending on which node the operator happened to reach.
//
// The answer is a set, not a single node, because ownership is genuinely
// plural for some resources: a parallel ingester runs on every eligible
// node (ingester HA), a vault has followers as well as a leader. Resolvers
// that today can only produce one owner return a one-element slice.
//
// Return values:
//   - (owners, nil) with len > 0 — route to one of these nodes.
//   - (nil, nil) — no routing decision available (resource ID unparseable,
//     ownership not yet reported). The request executes locally and the
//     handler produces its own domain error.
//   - (nil, err) — a definitive failure. ErrResourceNotFound maps to
//     CodeNotFound; anything else to CodeFailedPrecondition.
//
// Owners MUST be returned in a deterministic order (sort them): the
// interceptor picks owners[0] when the local node is not among them, and
// every node must pick the same one.
type OwnerResolver interface {
	ResolveOwners(ctx context.Context, resourceID string) ([]string, error)
}

// OwnerResolvers maps each ResourceKind to the resolver that owns it.
// Built once at startup; adding a resource kind is one entry here plus one
// Resource declaration per procedure in routes.go — the interceptor itself
// does not change.
type OwnerResolvers map[ResourceKind]OwnerResolver
