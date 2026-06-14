package routing

import (
	"context"
	"errors"
	"sync"

	"gastrolog/internal/pipeline/segmentation"
)

// errVaultUnwired is returned on a record's Ack when its destination vault was
// unregistered after the route matched but before delivery completed.
var errVaultUnwired = errors.New("vault unwired during delivery")

// vaultSink is the routing-stage fan-out target for one vault. Workers may hold
// a *vaultSink across UnregisterVault; revoke marks the sink dead and waits for
// in-flight deliveries so segmentation can close the input channel safely.
type vaultSink struct {
	mu   sync.Mutex
	ch   chan<- segmentation.Input
	dead bool

	inflight sync.WaitGroup
}

func newVaultSink(ch chan<- segmentation.Input) *vaultSink {
	return &vaultSink{ch: ch}
}

func (s *vaultSink) deliver(ctx context.Context, item segmentation.Input) bool {
	s.inflight.Add(1)
	defer s.inflight.Done()

	s.mu.Lock()
	dead := s.dead
	ch := s.ch
	s.mu.Unlock()

	if dead {
		sendAck(item.Ack, errVaultUnwired)
		return false
	}

	select {
	case ch <- item:
		return true
	case <-ctx.Done():
		sendAck(item.Ack, ctx.Err())
		return false
	}
}

// revoke marks the sink unwired and blocks until workers finish any delivery
// that started before the revoke call.
func (s *vaultSink) revoke() {
	s.mu.Lock()
	s.dead = true
	s.mu.Unlock()
	s.inflight.Wait()
}
