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

// deliver hands item to the vault's segmentation queue. It returns nil on
// success, errVaultUnwired when the sink was revoked (vault unregistered
// mid-flight — the caller should keep fanning out to its remaining sinks), or
// the context error when ctx is done (the caller should stop: every subsequent
// send would fail the same way). On failure the item's ack, if any, is nacked
// here so the caller never owes it a result.
func (s *vaultSink) deliver(ctx context.Context, item segmentation.Input) error {
	s.inflight.Add(1)
	defer s.inflight.Done()

	s.mu.Lock()
	dead := s.dead
	ch := s.ch
	s.mu.Unlock()

	if dead {
		sendAck(item.Ack, errVaultUnwired)
		return errVaultUnwired
	}

	select {
	case ch <- item:
		return nil
	case <-ctx.Done():
		sendAck(item.Ack, ctx.Err())
		return ctx.Err()
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
