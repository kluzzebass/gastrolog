package syslog

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"gastrolog/internal/orchestrator"
)

func TestSyslogUDPRFC3164(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{UDPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Send RFC 3164 message.
	conn, err := net.Dial("udp", recv.UDPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	msg := "<34>Jan 15 10:22:15 router01 kernel: Interface eth0 down"
	conn.Write([]byte(msg))

	select {
	case m := <-out:
		if string(m.Raw) != msg {
			t.Errorf("expected raw %q, got %q", msg, m.Raw)
		}
		if m.Attrs["facility"] != "4" {
			t.Errorf("expected facility 4, got %q", m.Attrs["facility"])
		}
		if m.Attrs["severity"] != "2" {
			t.Errorf("expected severity 2, got %q", m.Attrs["severity"])
		}
		if m.Attrs["facility_name"] != "auth" {
			t.Errorf("expected facility_name auth, got %q", m.Attrs["facility_name"])
		}
		if m.Attrs["severity_name"] != "crit" {
			t.Errorf("expected severity_name crit, got %q", m.Attrs["severity_name"])
		}
		if m.Attrs["hostname"] != "router01" {
			t.Errorf("expected hostname router01, got %q", m.Attrs["hostname"])
		}
		if m.Attrs["app_name"] != "kernel" {
			t.Errorf("expected app_name kernel, got %q", m.Attrs["app_name"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestSyslogUDPRFC5424(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{UDPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("udp", recv.UDPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// RFC 5424 message.
	msg := "<165>1 2023-08-25T14:30:45.123Z server1 myapp 1234 ID001 - System restart initiated"
	conn.Write([]byte(msg))

	select {
	case m := <-out:
		if string(m.Raw) != msg {
			t.Errorf("expected raw %q, got %q", msg, m.Raw)
		}
		// Priority 165 = facility 20 (local4), severity 5 (notice)
		if m.Attrs["facility"] != "20" {
			t.Errorf("expected facility 20, got %q", m.Attrs["facility"])
		}
		if m.Attrs["severity"] != "5" {
			t.Errorf("expected severity 5, got %q", m.Attrs["severity"])
		}
		if m.Attrs["facility_name"] != "local4" {
			t.Errorf("expected facility_name local4, got %q", m.Attrs["facility_name"])
		}
		if m.Attrs["severity_name"] != "notice" {
			t.Errorf("expected severity_name notice, got %q", m.Attrs["severity_name"])
		}
		if m.Attrs["version"] != "1" {
			t.Errorf("expected version 1, got %q", m.Attrs["version"])
		}
		if m.Attrs["hostname"] != "server1" {
			t.Errorf("expected hostname server1, got %q", m.Attrs["hostname"])
		}
		if m.Attrs["app_name"] != "myapp" {
			t.Errorf("expected app_name myapp, got %q", m.Attrs["app_name"])
		}
		if m.Attrs["proc_id"] != "1234" {
			t.Errorf("expected proc_id 1234, got %q", m.Attrs["proc_id"])
		}
		if m.Attrs["msg_id"] != "ID001" {
			t.Errorf("expected msg_id ID001, got %q", m.Attrs["msg_id"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestSyslogTCPNewlineDelimited(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{TCPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", recv.TCPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send two newline-delimited messages.
	msg1 := "<34>Jan 15 10:22:15 host1 app1: message 1"
	msg2 := "<35>Jan 15 10:22:16 host2 app2: message 2"
	conn.Write([]byte(msg1 + "\n" + msg2 + "\n"))

	// Receive first message.
	select {
	case m := <-out:
		if string(m.Raw) != msg1 {
			t.Errorf("expected raw %q, got %q", msg1, m.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message 1")
	}

	// Receive second message.
	select {
	case m := <-out:
		if string(m.Raw) != msg2 {
			t.Errorf("expected raw %q, got %q", msg2, m.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message 2")
	}
}

func TestSyslogTCPOctetCounted(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{TCPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", recv.TCPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send octet-counted message: "length message"
	msg := "<34>Jan 15 10:22:15 host1 app1: test"
	octetCounted := fmt.Sprintf("%d %s", len(msg), msg)
	conn.Write([]byte(octetCounted))

	select {
	case m := <-out:
		if string(m.Raw) != msg {
			t.Errorf("expected raw %q, got %q", msg, m.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestSyslogMultipleUDPMessages(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 100)
	recv := New(Config{UDPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("udp", recv.UDPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send 10 messages.
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("<34>Jan 15 10:22:15 host app: message %d", i)
		conn.Write([]byte(msg))
	}

	// Receive all messages.
	received := 0
	timeout := time.After(2 * time.Second)
	for received < 10 {
		select {
		case <-out:
			received++
		case <-timeout:
			t.Fatalf("timed out, only received %d/10 messages", received)
		}
	}
}

func TestSyslogRemoteIP(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{UDPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("udp", recv.UDPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte("<34>test message"))

	select {
	case m := <-out:
		if m.Attrs["remote_ip"] != "127.0.0.1" {
			t.Errorf("expected remote_ip 127.0.0.1, got %q", m.Attrs["remote_ip"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestSyslogRFC3164WithPID(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{UDPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("udp", recv.UDPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// RFC 3164 with PID: "app[1234]: message"
	msg := "<34>Jan 15 10:22:15 myhost myapp[1234]: hello world"
	conn.Write([]byte(msg))

	select {
	case m := <-out:
		if m.Attrs["hostname"] != "myhost" {
			t.Errorf("expected hostname myhost, got %q", m.Attrs["hostname"])
		}
		if m.Attrs["app_name"] != "myapp" {
			t.Errorf("expected app_name myapp, got %q", m.Attrs["app_name"])
		}
		if m.Attrs["proc_id"] != "1234" {
			t.Errorf("expected proc_id 1234, got %q", m.Attrs["proc_id"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestSyslogNoPriority(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{UDPAddr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("udp", recv.UDPAddr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Message without priority.
	msg := "Just a plain message"
	conn.Write([]byte(msg))

	select {
	case m := <-out:
		if string(m.Raw) != msg {
			t.Errorf("expected raw %q, got %q", msg, m.Raw)
		}
		if _, ok := m.Attrs["facility"]; ok {
			t.Error("expected no facility attr")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestSyslogBothUDPAndTCP(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{
		UDPAddr: "127.0.0.1:0",
		TCPAddr: "127.0.0.1:0",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Send via UDP.
	udpConn, _ := net.Dial("udp", recv.UDPAddr().String())
	defer udpConn.Close()
	udpConn.Write([]byte("<34>UDP message"))

	// Send via TCP.
	tcpConn, _ := net.Dial("tcp", recv.TCPAddr().String())
	defer tcpConn.Close()
	tcpConn.Write([]byte("<35>TCP message\n"))

	// Receive both.
	received := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case m := <-out:
			received[string(m.Raw)] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out, received %d/2", i)
		}
	}

	if !received["<34>UDP message"] {
		t.Error("did not receive UDP message")
	}
	if !received["<35>TCP message"] {
		t.Error("did not receive TCP message")
	}
}
