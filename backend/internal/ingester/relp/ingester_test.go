package relp

import (
	"gastrolog/internal/glid"
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"runtime"
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
	t.Parallel()
	factory := NewFactory(nil)

	// Default addr.
	ing, err := factory(glid.New(), nil, nil)
	if err != nil {
		t.Fatalf("factory with nil params: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}

	// Custom addr.
	ing, err = factory(glid.New(), map[string]string{"addr": ":9514"}, nil)
	if err != nil {
		t.Fatalf("factory with custom addr: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}
}

func TestRELPSession(t *testing.T) {
	t.Parallel()
	out := make(chan orchestrator.IngestMessage, 10)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()

	// Wait for listener to start.
	deadline := time.Now().Add(2 * time.Second)
	var addr net.Addr
	for {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("listener did not start")
		}
		runtime.Gosched()
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
		if m.IngesterID != "test-relp" {
			t.Errorf("expected IngesterID test-relp, got %q", m.IngesterID)
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
	t.Parallel()
	out := make(chan orchestrator.IngestMessage, 10)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ing.Run(ctx, out)

	// Wait for listener.
	deadline := time.Now().Add(2 * time.Second)
	var addr net.Addr
	for {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("listener did not start")
		}
		runtime.Gosched()
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
	t.Parallel()
	out := make(chan orchestrator.IngestMessage, 10)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ing.Run(ctx, out)

	// Wait for listener.
	deadline := time.Now().Add(2 * time.Second)
	var addr net.Addr
	for {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("listener did not start")
		}
		runtime.Gosched()
	}

	// Connect and immediately close — should not crash the ingester.
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	conn.Close()

	// The ingester should still be running — retry until a new connection succeeds.
	retryDeadline := time.Now().Add(2 * time.Second)
	var conn2 net.Conn
	for {
		conn2, err = net.DialTimeout("tcp", addr.String(), 50*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(retryDeadline) {
			t.Fatalf("second dial failed (ingester may have crashed): %v", err)
		}
		runtime.Gosched()
	}
	if err != nil {
		t.Fatalf("second dial failed (ingester may have crashed): %v", err)
	}
	conn2.Close()

	cancel()
}

// generateTestCert creates a self-signed CA + server cert for TLS tests.
func generateTestCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	// Generate CA key and certificate.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	// Generate server key and certificate signed by CA.
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTemplate, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}

	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	srvKeyDER, _ := x509.MarshalECPrivateKey(srvKey)
	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER})

	cert, err := tls.X509KeyPair(srvCertPEM, srvKeyPEM)
	if err != nil {
		t.Fatalf("load keypair: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return cert, pool
}

func TestRELPTLS(t *testing.T) {
	t.Parallel()
	out := make(chan orchestrator.IngestMessage, 10)

	srvCert, caPool := generateTestCert(t)
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{srvCert},
		MinVersion:   tls.VersionTLS12,
	}

	ing := New(Config{
		ID:        "test-relp-tls",
		Addr:      "127.0.0.1:0",
		TLSConfig: tlsCfg,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ing.Run(ctx, out)

	// Wait for listener.
	deadline := time.Now().Add(2 * time.Second)
	var addr net.Addr
	for {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("listener did not start")
		}
		runtime.Gosched()
	}

	// Connect with TLS.
	conn, err := tls.Dial("tcp", addr.String(), &tls.Config{
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("TLS dial failed: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Open session.
	writeRELPFrame(conn, 1, "open", "relp_version=1\nrelp_software=test\ncommands=syslog")
	txnr, cmd, _, err := readRELPResponse(reader)
	if err != nil {
		t.Fatalf("read open response: %v", err)
	}
	if txnr != 1 || cmd != "rsp" {
		t.Fatalf("unexpected open response: txnr=%d cmd=%s", txnr, cmd)
	}

	// Send syslog message over TLS.
	syslogMsg := "<34>Jan 15 10:22:15 router01 kernel: TLS test message"
	writeRELPFrame(conn, 2, "syslog", syslogMsg)

	select {
	case m := <-out:
		if string(m.Raw) != syslogMsg {
			t.Errorf("expected raw %q, got %q", syslogMsg, m.Raw)
		}
		m.Ack <- nil
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	// Read ack.
	txnr, cmd, rspData, err := readRELPResponse(reader)
	if err != nil {
		t.Fatalf("read syslog ack: %v", err)
	}
	if txnr != 2 || cmd != "rsp" {
		t.Fatalf("unexpected ack: txnr=%d cmd=%s", txnr, cmd)
	}
	if !strings.Contains(rspData, "200 Ok") {
		t.Errorf("expected 200 Ok, got %q", rspData)
	}

	cancel()
}

func TestRELPProtocol(t *testing.T) {
	t.Parallel()

	t.Run("frame_round_trip", func(t *testing.T) {
		t.Parallel()
		// net.Pipe gives a synchronous bidirectional connection.
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		session := NewSession(server, server)
		reader := bufio.NewReader(client)

		type ackResult struct {
			txnr int
			cmd  string
			data string
			err  error
		}
		ackCh := make(chan ackResult, 1)

		// Client goroutine: open → drain open rsp → syslog → read syslog ack.
		go func() {
			writeRELPFrame(client, 1, "open", "relp_version=1\ncommands=syslog")
			readRELPResponse(reader) // drain open response
			writeRELPFrame(client, 2, "syslog", "test message")
			txnr, cmd, data, err := readRELPResponse(reader)
			ackCh <- ackResult{txnr, cmd, data, err}
		}()

		msg, err := session.ReceiveLog()
		if err != nil {
			t.Fatalf("ReceiveLog: %v", err)
		}

		if string(msg.Data) != "test message" {
			t.Errorf("expected 'test message', got %q", msg.Data)
		}
		if msg.Txnr != 2 {
			t.Errorf("expected txnr 2, got %d", msg.Txnr)
		}

		if err := session.AnswerOk(msg); err != nil {
			t.Fatalf("AnswerOk: %v", err)
		}

		ack := <-ackCh
		if ack.err != nil {
			t.Fatalf("read ack: %v", ack.err)
		}
		if ack.txnr != 2 || ack.cmd != "rsp" || !strings.Contains(ack.data, "200 Ok") {
			t.Errorf("unexpected ack: txnr=%d cmd=%s data=%q", ack.txnr, ack.cmd, ack.data)
		}
	})

	t.Run("zero_datalen_close", func(t *testing.T) {
		t.Parallel()
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		session := NewSession(server, server)
		reader := bufio.NewReader(client)

		go func() {
			writeRELPFrame(client, 1, "open", "relp_version=0\ncommands=syslog")
			readRELPResponse(reader) // drain open response
			// Send close with 0 datalen — "2 close 0\n"
			client.Write([]byte("2 close 0\n"))
			readRELPResponse(reader) // drain close response
		}()

		_, err := session.ReceiveLog()
		if err == nil {
			t.Fatal("expected EOF on close")
		}
	})
}
