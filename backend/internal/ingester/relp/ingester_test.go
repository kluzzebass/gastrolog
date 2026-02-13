package relp

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/orchestrator"
)

// writeRELPFrame writes a RELP frame: "TXNR SP COMMAND SP DATALEN SP DATA LF"
func writeRELPFrame(conn net.Conn, txnr int, command string, data string) {
	frame := fmt.Sprintf("%d %s %d %s\n", txnr, command, len(data), data)
	conn.Write([]byte(frame))
}

// readRELPResponse reads a RELP response frame and returns txnr, command, data.
// RELP frames: "TXNR SP COMMAND SP DATALEN SP DATA LF"
// DATA may contain newlines, so we must parse by DATALEN rather than reading lines.
func readRELPResponse(reader *bufio.Reader) (txnr int, command string, data string, err error) {
	// Read TXNR (digits until space).
	txnrStr, err := readToken(reader)
	if err != nil {
		return 0, "", "", fmt.Errorf("read txnr: %w", err)
	}
	txnr, err = strconv.Atoi(txnrStr)
	if err != nil {
		return 0, "", "", fmt.Errorf("invalid txnr %q: %w", txnrStr, err)
	}

	// Read COMMAND (until space).
	command, err = readToken(reader)
	if err != nil {
		return 0, "", "", fmt.Errorf("read command: %w", err)
	}

	// Read DATALEN (until space).
	datalenStr, err := readToken(reader)
	if err != nil {
		return 0, "", "", fmt.Errorf("read datalen: %w", err)
	}
	datalen, err := strconv.Atoi(datalenStr)
	if err != nil {
		return 0, "", "", fmt.Errorf("invalid datalen %q: %w", datalenStr, err)
	}

	// Read exactly DATALEN bytes of data.
	if datalen > 0 {
		buf := make([]byte, datalen)
		n := 0
		for n < datalen {
			nn, err := reader.Read(buf[n:])
			if err != nil {
				return 0, "", "", fmt.Errorf("read data: %w", err)
			}
			n += nn
		}
		data = string(buf)
	}

	// Consume trailing LF.
	b, err := reader.ReadByte()
	if err != nil {
		return txnr, command, data, nil // may not have trailing LF
	}
	if b != '\n' {
		reader.UnreadByte()
	}

	return txnr, command, data, nil
}

// readToken reads a space-delimited token from the reader.
func readToken(reader *bufio.Reader) (string, error) {
	var token []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return string(token), err
		}
		if b == ' ' {
			return string(token), nil
		}
		token = append(token, b)
	}
}

func TestRELPFactory(t *testing.T) {
	factory := NewFactory()

	// Default addr.
	ing, err := factory("test", nil, nil)
	if err != nil {
		t.Fatalf("factory with nil params: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}

	// Custom addr.
	ing, err = factory("test", map[string]string{"addr": ":9514"}, nil)
	if err != nil {
		t.Fatalf("factory with custom addr: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}
}

func TestRELPSession(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()

	// Wait for listener to start.
	var addr net.Addr
	for i := 0; i < 50; i++ {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == nil {
		t.Fatal("listener did not start")
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send RELP open command.
	offerData := "relp_version=0\nrelp_software=test\ncommands=syslog"
	writeRELPFrame(conn, 1, "open", offerData)

	// Read open response.
	txnr, cmd, _, err := readRELPResponse(reader)
	if err != nil {
		t.Fatalf("read open response: %v", err)
	}
	if txnr != 1 || cmd != "rsp" {
		t.Fatalf("unexpected open response: txnr=%d cmd=%s", txnr, cmd)
	}

	// Send a syslog message.
	syslogMsg := "<34>Jan 15 10:22:15 router01 kernel: Interface eth0 down"
	writeRELPFrame(conn, 2, "syslog", syslogMsg)

	// Read the ingested message and send ack (simulating orchestrator).
	select {
	case m := <-out:
		if string(m.Raw) != syslogMsg {
			t.Errorf("expected raw %q, got %q", syslogMsg, m.Raw)
		}
		if m.Attrs["ingester_type"] != "relp" {
			t.Errorf("expected ingester_type relp, got %q", m.Attrs["ingester_type"])
		}
		if m.Attrs["ingester_id"] != "test-relp" {
			t.Errorf("expected ingester_id test-relp, got %q", m.Attrs["ingester_id"])
		}
		if m.Attrs["facility"] != "4" {
			t.Errorf("expected facility 4, got %q", m.Attrs["facility"])
		}
		if m.Attrs["severity"] != "2" {
			t.Errorf("expected severity 2, got %q", m.Attrs["severity"])
		}
		if m.Attrs["hostname"] != "router01" {
			t.Errorf("expected hostname router01, got %q", m.Attrs["hostname"])
		}
		if m.Attrs["remote_ip"] != "127.0.0.1" {
			t.Errorf("expected remote_ip 127.0.0.1, got %q", m.Attrs["remote_ip"])
		}
		// Simulate orchestrator ack (write succeeded).
		m.Ack <- nil
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	// Read the RELP ack for the syslog message.
	txnr, cmd, rspData, err := readRELPResponse(reader)
	if err != nil {
		t.Fatalf("read syslog ack: %v", err)
	}
	if txnr != 2 || cmd != "rsp" {
		t.Fatalf("unexpected syslog ack: txnr=%d cmd=%s", txnr, cmd)
	}
	if !strings.Contains(rspData, "200 Ok") {
		t.Errorf("expected 200 Ok in ack data, got %q", rspData)
	}

	// Send close.
	writeRELPFrame(conn, 3, "close", "")

	cancel()
}

func TestRELPMultipleMessages(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ing.Run(ctx, out)

	// Wait for listener.
	var addr net.Addr
	for i := 0; i < 50; i++ {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == nil {
		t.Fatal("listener did not start")
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Open session.
	writeRELPFrame(conn, 1, "open", "relp_version=0\nrelp_software=test\ncommands=syslog")
	readRELPResponse(reader) // consume open response

	// Send and receive messages one at a time (RELP is sequential per connection).
	for i := 2; i <= 4; i++ {
		msg := fmt.Sprintf("<34>Jan 15 10:22:15 host app: message %d", i)
		writeRELPFrame(conn, i, "syslog", msg)

		select {
		case m := <-out:
			expected := fmt.Sprintf("<34>Jan 15 10:22:15 host app: message %d", i)
			if string(m.Raw) != expected {
				t.Errorf("message %d: expected %q, got %q", i, expected, m.Raw)
			}
			// Simulate orchestrator ack.
			m.Ack <- nil
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}

		// Read ack.
		txnr, _, _, err := readRELPResponse(reader)
		if err != nil {
			t.Fatalf("read ack for message %d: %v", i, err)
		}
		if txnr != i {
			t.Errorf("expected ack txnr %d, got %d", i, txnr)
		}
	}

	cancel()
}

func TestRELPConnectionClose(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ing.Run(ctx, out)

	// Wait for listener.
	var addr net.Addr
	for i := 0; i < 50; i++ {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == nil {
		t.Fatal("listener did not start")
	}

	// Connect and immediately close — should not crash the ingester.
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	conn.Close()

	// Give time for the handler to process the close.
	time.Sleep(100 * time.Millisecond)

	// The ingester should still be running — try another connection.
	conn2, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("second dial failed (ingester may have crashed): %v", err)
	}
	conn2.Close()

	cancel()
}
