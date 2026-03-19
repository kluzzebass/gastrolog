package routing

import "context"

// Context keys for routing metadata. These are transport-agnostic: any
// caller (HTTP middleware, internal Go, unix socket) can set them.
type contextKey int

const (
	targetNodeKey contextKey = iota
	forwardedKey
)

// WithTargetNode returns a context that targets a specific cluster node.
// The routing interceptor will forward the request to this node instead
// of applying the default strategy. If targetNodeID matches the local
// node, the request executes locally without a network hop.
func WithTargetNode(ctx context.Context, targetNodeID string) context.Context {
	return context.WithValue(ctx, targetNodeKey, targetNodeID)
}

// TargetNode returns the explicit target node from the context, or empty
// string if no target was set.
func TargetNode(ctx context.Context) string {
	v, _ := ctx.Value(targetNodeKey).(string)
	return v
}

// WithForwarded marks the context as already-forwarded. The routing
// interceptor checks this flag to prevent forwarding loops: if set,
// the request always executes locally regardless of strategy or target.
func WithForwarded(ctx context.Context) context.Context {
	return context.WithValue(ctx, forwardedKey, true)
}

// IsForwarded returns true if the request has already been forwarded
// from another node (loop prevention).
func IsForwarded(ctx context.Context) bool {
	v, _ := ctx.Value(forwardedKey).(bool)
	return v
}
