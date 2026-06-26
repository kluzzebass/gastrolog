package multiraft

import (
	"net"
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
