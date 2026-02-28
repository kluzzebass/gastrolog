package cluster

import (
	"context"
	"sync"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// subscriber is a registered broadcast listener with a unique ID for removal.
type subscriber struct {
	id uint64
	fn func(*gastrologv1.BroadcastMessage)
}

// subscriberRegistry manages broadcast message subscribers.
type subscriberRegistry struct {
	mu     sync.RWMutex
	subs   []subscriber
	nextID uint64
}

// subscribe registers a callback and returns an unsubscribe function.
func (r *subscriberRegistry) subscribe(fn func(*gastrologv1.BroadcastMessage)) func() {
	r.mu.Lock()
	id := r.nextID
	r.nextID++
	r.subs = append(r.subs, subscriber{id: id, fn: fn})
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for i, s := range r.subs {
			if s.id == id {
				r.subs = append(r.subs[:i], r.subs[i+1:]...)
				return
			}
		}
	}
}

// dispatch calls all registered subscribers with the message.
func (r *subscriberRegistry) dispatch(msg *gastrologv1.BroadcastMessage) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.subs {
		s.fn(msg)
	}
}

// Subscribe registers a callback that will be invoked for every broadcast
// message received from peers. Returns a function to unsubscribe.
// Callbacks are invoked synchronously within the RPC handler — they should
// be fast (store to a map, enqueue work, etc.).
func (s *Server) Subscribe(fn func(*gastrologv1.BroadcastMessage)) func() {
	return s.subscribers.subscribe(fn)
}

// broadcast handles the Broadcast RPC — dispatches the message to all
// local subscribers.
func (s *Server) broadcast(_ context.Context, req *gastrologv1.BroadcastRequest) (*gastrologv1.BroadcastResponse, error) {
	msg := req.GetMessage()
	if msg == nil {
		return nil, status.Error(codes.InvalidArgument, "missing message")
	}
	s.subscribers.dispatch(msg)
	return &gastrologv1.BroadcastResponse{}, nil
}

// broadcastHandler is the gRPC MethodDesc handler for the Broadcast RPC.
func broadcastHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.BroadcastRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.broadcast(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/Broadcast",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.broadcast(ctx, req.(*gastrologv1.BroadcastRequest))
	}
	return interceptor(ctx, req, info, handler)
}
