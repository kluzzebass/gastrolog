// Package syslog provides a syslog ingester that accepts messages via UDP and TCP.
package syslog

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"gastrolog/internal/ingester/syslogparse"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Ingester accepts syslog messages via UDP and/or TCP.
// It implements orchestrator.Ingester.
//
// Supports both RFC 3164 (BSD) and RFC 5424 (IETF) formats with auto-detection.
// Messages are parsed and relevant fields extracted into attributes.
type Ingester struct {
	id      string
	udpAddr string
	tcpAddr string
	out     chan<- orchestrator.IngestMessage
	logger  *slog.Logger

	mu          sync.Mutex
	udpConn     *net.UDPConn
	tcpListener net.Listener
}

// Config holds syslog ingester configuration.
type Config struct {
	// ID is the ingester's config identifier.
	ID string

	// UDPAddr is the UDP address to listen on (e.g., ":514").
	// Empty string disables UDP.
	UDPAddr string

	// TCPAddr is the TCP address to listen on (e.g., ":514").
	// Empty string disables TCP.
	TCPAddr string

	// Logger for structured logging.
	Logger *slog.Logger
}

// New creates a new syslog ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		id:      cfg.ID,
		udpAddr: cfg.UDPAddr,
		tcpAddr: cfg.TCPAddr,
		logger:  logging.Default(cfg.Logger).With("component", "ingester", "type", "syslog"),
	}
}

// Run starts the syslog listeners and blocks until ctx is cancelled.
func (r *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	r.out = out

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Start UDP listener if configured.
	if r.udpAddr != "" {
		wg.Go(func() {
			if err := r.runUDP(ctx); err != nil {
				errCh <- err
			}
		})
	}

	// Start TCP listener if configured.
	if r.tcpAddr != "" {
		wg.Go(func() {
			if err := r.runTCP(ctx); err != nil {
				errCh <- err
			}
		})
	}

	if r.udpAddr == "" && r.tcpAddr == "" {
		return errors.New("syslog ingester: no UDP or TCP address configured")
	}

	// Wait for context cancellation or error.
	select {
	case <-ctx.Done():
		r.logger.Info("syslog ingester stopping")
		r.shutdown()
		wg.Wait()
		return nil
	case err := <-errCh:
		r.logger.Info("syslog ingester stopping", "error", err)
		r.shutdown()
		wg.Wait()
		return err
	}
}

// shutdown closes all listeners.
func (r *Ingester) shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.udpConn != nil {
		_ = r.udpConn.Close()
		r.udpConn = nil
	}
	if r.tcpListener != nil {
		_ = r.tcpListener.Close()
		r.tcpListener = nil
	}
}

// runUDP handles UDP syslog messages.
func (r *Ingester) runUDP(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", r.udpAddr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.udpConn = conn
	r.mu.Unlock()

	r.logger.Info("syslog UDP listener starting", "addr", conn.LocalAddr().String())

	buf := make([]byte, 65536) // Max UDP packet size
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Set read deadline to allow checking context.
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))

		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			r.logger.Warn("UDP read error", "error", err)
			continue
		}

		if n == 0 {
			continue
		}

		msg := r.buildMessage(buf[:n], remoteAddr.IP.String())
		select {
		case r.out <- msg:
		case <-ctx.Done():
			return nil
		}
	}
}

// runTCP handles TCP syslog connections.
func (r *Ingester) runTCP(ctx context.Context) error {
	listener, err := net.Listen("tcp", r.tcpAddr)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.tcpListener = listener
	r.mu.Unlock()

	r.logger.Info("syslog TCP listener starting", "addr", listener.Addr().String())

	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		default:
		}

		// Set accept deadline to allow checking context.
		_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))

		conn, err := listener.Accept()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			r.logger.Warn("TCP accept error", "error", err)
			continue
		}

		wg.Go(func() {
			defer func() { _ = conn.Close() }()
			r.handleTCPConn(ctx, conn)
		})
	}
}

// handleTCPConn handles a single TCP connection.
// TCP syslog uses either newline-delimited or octet-counted framing.
func (r *Ingester) handleTCPConn(ctx context.Context, conn net.Conn) {
	remoteIP := ""
	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = tcpAddr.IP.String()
	}

	reader := bufio.NewReader(conn)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		line, err := r.readFrame(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !isTimeout(err) {
				r.logger.Debug("TCP read error", "error", err)
			}
			return
		}

		if len(line) == 0 {
			continue
		}

		msg := r.buildMessage(line, remoteIP)
		select {
		case r.out <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (r *Ingester) readFrame(reader *bufio.Reader) ([]byte, error) {
	firstByte, err := reader.Peek(1)
	if err != nil {
		return nil, err
	}

	if firstByte[0] >= '0' && firstByte[0] <= '9' {
		return r.readOctetCounted(reader)
	}

	return readNewlineDelimited(reader)
}

func readNewlineDelimited(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = trimCRLF(line)
	return line, nil
}

func trimCRLF(line []byte) []byte {
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// readOctetCounted reads an octet-counted syslog message.
// Format: "123 <message>" where 123 is the length of <message>.
func (r *Ingester) readOctetCounted(reader *bufio.Reader) ([]byte, error) {
	// Read the length prefix.
	var length int
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == ' ' {
			break
		}
		if b < '0' || b > '9' {
			return nil, errors.New("invalid octet count")
		}
		length = length*10 + int(b-'0')
		if length > 1<<20 { // 1MB sanity limit
			return nil, errors.New("octet count too large")
		}
	}

	// Read the message.
	msg := make([]byte, length)
	_, err := io.ReadFull(reader, msg)
	return msg, err
}

// UDPAddr returns the UDP listener address. Only valid after Run() has started.
func (r *Ingester) UDPAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.udpConn == nil {
		return nil
	}
	return r.udpConn.LocalAddr()
}

// TCPAddr returns the TCP listener address. Only valid after Run() has started.
func (r *Ingester) TCPAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tcpListener == nil {
		return nil
	}
	return r.tcpListener.Addr()
}

// buildMessage parses a syslog message using syslogparse and wraps it in an IngestMessage.
func (r *Ingester) buildMessage(data []byte, remoteIP string) orchestrator.IngestMessage {
	attrs, sourceTS := syslogparse.ParseMessage(data, remoteIP)
	attrs["ingester_type"] = "syslog"
	attrs["ingester_id"] = r.id

	return orchestrator.IngestMessage{
		Attrs:    attrs,
		Raw:      data,
		SourceTS: sourceTS,
		IngestTS: time.Now(),
	}
}
