package system

import (
	"context"
	"testing"
	"time"
)

// closingStore is a store whose Close, like a Raft-backed store's, waits for
// work that reads through the proxy in front of it: a Raft FSM applying an
// entry while shutdown waits for it.
type closingStore struct {
	Store
	proxy *StoreProxy
}

func (s *closingStore) Load(context.Context) (*System, error) { return &System{}, nil }

func (s *closingStore) Close() error {
	applied := make(chan struct{})
	go func() {
		_, _ = s.proxy.Load(context.Background())
		close(applied)
	}()
	<-applied
	return nil
}

func TestCloseDoesNotHoldTheProxyAgainstTheInnerStoresShutdown(t *testing.T) {
	t.Parallel()
	inner := &closingStore{}
	proxy := NewStoreProxy(inner)
	inner.proxy = proxy

	closed := make(chan error, 1)
	go func() { closed <- proxy.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked: the inner store's shutdown waits on a read through the proxy Close holds")
	}
}
