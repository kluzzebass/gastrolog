package multiraft

import (
	"net"
	"sync"
	"testing"
)

func TestInboundLaneRegistryDeliver(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	reg := NewInboundLaneRegistry(ln.Addr())
	t.Cleanup(func() { reg.Close() })

	group := "config"
	listener := reg.Listener(group)

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	acceptCh := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		acceptCh <- conn
	}()

	if !reg.Deliver(group, server) {
		t.Fatal("Deliver returned false for registered listener")
	}

	var accepted net.Conn
	select {
	case accepted = <-acceptCh:
	case err := <-acceptErr:
		t.Fatalf("Accept: %v", err)
	}
	t.Cleanup(func() { _ = accepted.Close() })

	if !reg.Deliver("missing-group", client) {
		// client should still be open; unknown group returns false
	} else {
		t.Fatal("Deliver returned true for unknown group")
	}
}

// A group's gRPC server closes its own listener on stop, and DestroyGroup
// removes the lane right after — the registry must absorb the second close
// instead of panicking a node in the middle of a vault delete.
func TestInboundLaneRemoveAfterListenerClose(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	reg := NewInboundLaneRegistry(ln.Addr())
	listener := reg.Listener("doomed")

	if err := listener.Close(); err != nil {
		t.Fatalf("listener Close: %v", err)
	}
	reg.Remove("doomed") // pre-fix: close of closed channel

	if err := listener.Close(); err == nil {
		t.Fatal("second listener Close reported success")
	}

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	if reg.Deliver("doomed", server) {
		t.Fatal("Deliver returned true for a removed lane")
	}
	if _, err := listener.Accept(); err == nil {
		t.Fatal("Accept on a closed lane returned a conn")
	}
}

// Every shutdown path may race every other: the server's own listener Close,
// the registry Remove that follows DestroyGroup, and the registry-wide Close
// at node shutdown. None of them may panic, whatever the interleaving.
func TestInboundLaneCloseStorm(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	for range 50 {
		reg := NewInboundLaneRegistry(ln.Addr())
		listeners := make([]net.Listener, 4)
		for i := range listeners {
			listeners[i] = reg.Listener(string(rune('a' + i)))
		}
		var wg sync.WaitGroup
		for i := range listeners {
			l := listeners[i]
			g := string(rune('a' + i))
			wg.Add(3)
			go func() { defer wg.Done(); _ = l.Close() }()
			go func() { defer wg.Done(); reg.Remove(g) }()
			go func() { defer wg.Done(); reg.Close() }()
		}
		wg.Wait()
	}
}
