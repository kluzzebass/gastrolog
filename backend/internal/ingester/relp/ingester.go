// Package relp provides a RELP (Reliable Event Logging Protocol) ingester.
// RELP is a TCP-based reliable syslog transport with transaction-based
// acknowledgments, commonly used by rsyslog.
package relp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/ingester/limits"
	"gastrolog/internal/ingester/syslogparse"
	"gastrolog/internal/logging"
	"gastrolog/internal/logging/comp"
	"gastrolog/internal/panicguard"
	"gastrolog/internal/pipeline/ingestion"
)

// refusedLogInterval spaces the "listener at capacity" warning. A peer that
// hammers a full listener would otherwise write one line per refused
// connection, burying the condition it is reporting.
const refusedLogInterval = 10 * time.Second

// Ingester accepts syslog messages via the RELP protocol.
// It implements ingestion.Ingester.
//
// RELP provides reliable delivery: each message is acknowledged only after
// it has been written to the chunk store, so the sender knows exactly which
// messages were processed.
type Ingester struct {
	id     string
	addr   string
	logger *slog.Logger

	tlsConfig *tls.Config

	mu       sync.Mutex
	listener net.Listener

	// conns caps concurrent connections; refusedLog keeps a peer that
	// hammers the cap from filling the log with one line per refusal.
	conns      *limits.ConnLimiter
	refusedLog logging.Throttle

	// frameTimeout bounds how long a sender may take to finish a frame it
	// has started. Tests shorten it; nothing else sets it.
	frameTimeout time.Duration

	// pressureGate throttles socket reads when the ingest pipeline is backed up.
	// The ack-gated message flow already provides indirect backpressure; this
	// gate provides a faster signal via the TCP window before senders queue up
	// on pending ACKs. Injected by the orchestrator.
	pressureGate *chanwatch.PressureGate
}

// SetPressureGate wires the orchestrator's pressure gate into the ingester.
// Implements ingestion.PressureAware.
func (r *Ingester) SetPressureGate(gate *chanwatch.PressureGate) {
	r.pressureGate = gate
}

// Config holds RELP ingester configuration.
type Config struct {
	// ID is the ingester's config identifier.
	ID string

	// Addr is the TCP address to listen on (e.g., ":2514").
	Addr string

	// TLSConfig, if non-nil, wraps accepted connections with TLS.
	// For mutual TLS, set ClientAuth and ClientCAs on the config.
	TLSConfig *tls.Config

	// Logger for structured logging.
	Logger *slog.Logger
}

// New creates a new RELP ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		id:           cfg.ID,
		addr:         cfg.Addr,
		tlsConfig:    cfg.TLSConfig,
		conns:        limits.NewConnLimiter(limits.MaxConnections),
		frameTimeout: limits.FrameTimeout,
		refusedLog:   logging.Throttle{Interval: refusedLogInterval},
		logger:       comp.Ingester.Sub("relp").Desc("RELP ingester — TCP transport with transaction-based acknowledgments (rsyslog-compatible).").Apply(logging.Default(cfg.Logger)),
	}
}

// Run starts the RELP TCP listener and blocks until ctx is cancelled.
func (r *Ingester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	listener, err := net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.listener = listener
	r.mu.Unlock()

	proto := "TCP"
	if r.tlsConfig != nil {
		proto = "TLS"
	}
	r.logger.Info("RELP ingester starting", "addr", listener.Addr().String(), "proto", proto)

	var wg sync.WaitGroup
	defer func() {
		_ = listener.Close()
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("RELP ingester stopping")
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
				return nil
			}
			r.logger.Warn("RELP accept error", "error", err)
			continue
		}

		remote := conn.RemoteAddr().String()
		// Past the cap, refuse immediately: a peer opening connections
		// faster than they close must not exhaust the node's file
		// descriptors or goroutines.
		if !r.conns.Acquire() {
			_ = conn.Close()
			if n, ok := r.refusedLog.Allow("conn-limit"); ok {
				r.logger.Warn("RELP connection refused: listener at capacity",
					"remote", remote, "max_connections", limits.MaxConnections, "suppressed", n)
			}
			continue
		}

		wg.Go(func() {
			defer r.conns.Release()
			// A panic on a hostile frame costs this connection, not the
			// node and every other vault and ingester running on it.
			defer panicguard.Recover(r.logger, "RELP connection", "remote", remote)
			r.handleConn(ctx, conn, out)
		})
	}
}

// Addr returns the listener address. Only valid after Run() has started.
func (r *Ingester) Addr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listener == nil {
		return nil
	}
	return r.listener.Addr()
}

// handleConn handles a single RELP connection.
func (r *Ingester) handleConn(ctx context.Context, conn net.Conn, out chan<- ingestion.IngesterMessage) {
	defer func() { _ = conn.Close() }()

	remoteIP := ""
	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = tcpAddr.IP.String()
	}

	// A sender that starts a frame must finish it. Idle time between
	// frames stays unbounded: a relay on a quiet host holds an open
	// connection for hours and is not the thing being defended against.
	framed := limits.NewFrameConn(conn, r.frameTimeout)

	// Wrap with TLS if configured.
	var fd io.ReadWriter = framed
	if r.tlsConfig != nil {
		tlsConn := tls.Server(framed, r.tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			r.logger.Debug("RELP TLS handshake failed", "error", err, "remote", remoteIP)
			return
		}
		framed.Done()
		fd = tlsConn
	}

	session := NewSession(fd, fd)
	// Clear the deadline as each frame completes rather than once per
	// ReceiveLog: the first ReceiveLog reads the open handshake and then
	// blocks on the next frame, so a per-call boundary would hold the
	// handshake's deadline over a sender that is simply idle.
	session.OnFrame(framed.Done)

	r.logger.Debug("RELP session established", "remote", remoteIP)

	for {
		if ctx.Err() != nil {
			return
		}
		// Backpressure: pause reads while the pipeline is backed up. The
		// TCP window closes, RELP senders back off at the transport layer
		// instead of queuing on pending ACKs.
		if r.pressureGate != nil {
			_ = r.pressureGate.Wait(ctx)
		}
		if ctx.Err() != nil {
			return
		}
		if !r.receiveAndForward(ctx, session, out, remoteIP) {
			return
		}
	}
}

// receiveAndForward reads one RELP message, forwards it to the pipeline,
// and sends the sender the ack/error reply. Returns false when the caller
// should stop the connection loop (EOF, ctx cancel, or transport error).
func (r *Ingester) receiveAndForward(
	ctx context.Context,
	session *Session,
	out chan<- ingestion.IngesterMessage,
	remoteIP string,
) bool {
	msg, err := session.ReceiveLog()
	if err != nil {
		switch {
		case errors.Is(err, ErrOversizeFrame):
			if n, ok := r.refusedLog.Allow("oversize-frame"); ok {
				r.logger.Warn("RELP frame rejected", "error", err, "remote", remoteIP,
					"max_frame_bytes", limits.MaxFrameBytes, "suppressed", n)
			}
		case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		default:
			r.logger.Debug("RELP receive ended", "error", err, "remote", remoteIP)
		}
		return false
	}

	attrs, _ := syslogparse.ParseMessage(msg.Data, remoteIP)
	attrs["ingester_type"] = "relp"

	// Use ack channel for end-to-end delivery guarantee:
	// the orchestrator sends nil/error after writing to chunk store.
	ack := make(chan error, 1)

	// SourceTS not set — syslog timestamps are unreliable.
	// The timestamp digester extracts SourceTS during digestion.
	ingestMsg := ingestion.IngesterMessage{
		Attrs:      attrs,
		Raw:        msg.Data,
		IngestTS:   time.Now(),
		IngesterID: r.id,
		Ack:        ack,
	}

	select {
	case out <- ingestMsg:
	case <-ctx.Done():
		return false
	}

	// Wait for write confirmation before acknowledging to RELP sender.
	select {
	case writeErr := <-ack:
		return r.sendAnswer(session, msg, writeErr)
	case <-ctx.Done():
		return false
	}
}

// sendAnswer delivers the appropriate RELP response to the sender.
// Returns false if the answer transport fails (caller should exit loop).
func (r *Ingester) sendAnswer(session *Session, msg *Message, writeErr error) bool {
	if writeErr != nil {
		if err := session.AnswerError(msg, writeErr.Error()); err != nil {
			r.logger.Debug("RELP answer error failed", "error", err)
			return false
		}
		return true
	}
	if err := session.AnswerOk(msg); err != nil {
		r.logger.Debug("RELP answer ok failed", "error", err)
		return false
	}
	return true
}
