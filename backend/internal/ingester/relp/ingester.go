// Package relp provides a RELP (Reliable Event Logging Protocol) ingester.
// RELP is a TCP-based reliable syslog transport with transaction-based
// acknowledgments, commonly used by rsyslog.
package relp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	gorelp "github.com/thierry-f-78/go-relp"

	"gastrolog/internal/ingester/syslogparse"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Ingester accepts syslog messages via the RELP protocol.
// It implements orchestrator.Ingester.
//
// RELP provides reliable delivery: each message is acknowledged only after
// it has been written to the chunk store, so the sender knows exactly which
// messages were processed.
type Ingester struct {
	id     string
	addr   string
	logger *slog.Logger

	mu       sync.Mutex
	listener net.Listener
}

// Config holds RELP ingester configuration.
type Config struct {
	// ID is the ingester's config identifier.
	ID string

	// Addr is the TCP address to listen on (e.g., ":2514").
	Addr string

	// Logger for structured logging.
	Logger *slog.Logger
}

// New creates a new RELP ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		id:     cfg.ID,
		addr:   cfg.Addr,
		logger: logging.Default(cfg.Logger).With("component", "ingester", "type", "relp"),
	}
}

// Run starts the RELP TCP listener and blocks until ctx is cancelled.
func (r *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	listener, err := net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.listener = listener
	r.mu.Unlock()

	r.logger.Info("RELP listener starting", "addr", listener.Addr().String())

	var wg sync.WaitGroup
	defer func() {
		listener.Close()
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
		listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))

		conn, err := listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			r.logger.Warn("RELP accept error", "error", err)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			r.handleConn(ctx, conn, out)
		}()
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
func (r *Ingester) handleConn(ctx context.Context, conn net.Conn, out chan<- orchestrator.IngestMessage) {
	defer conn.Close()

	remoteIP := ""
	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = tcpAddr.IP.String()
	}

	opts, err := gorelp.ValidateOptions(&gorelp.Options{
		Tls: gorelp.Opt_tls_disabled,
	})
	if err != nil {
		r.logger.Error("RELP options validation failed", "error", err)
		return
	}

	session, err := gorelp.NewTcp(conn, opts)
	if err != nil {
		r.logger.Debug("RELP session setup failed", "error", err, "remote", remoteIP)
		return
	}
	defer session.Close()

	r.logger.Debug("RELP session established", "remote", remoteIP)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := session.ReceiveLog()
		if err != nil {
			// ReceiveLog returns error on close or protocol error.
			if !errors.Is(err, net.ErrClosed) {
				r.logger.Debug("RELP receive ended", "error", err, "remote", remoteIP)
			}
			return
		}

		attrs, sourceTS := syslogparse.ParseMessage(msg.Data, remoteIP)
		attrs["ingester_type"] = "relp"
		attrs["ingester_id"] = r.id

		// Use ack channel for end-to-end delivery guarantee:
		// the orchestrator sends nil/error after writing to chunk store.
		ack := make(chan error, 1)

		ingestMsg := orchestrator.IngestMessage{
			Attrs:    attrs,
			Raw:      msg.Data,
			SourceTS: sourceTS,
			IngestTS: time.Now(),
			Ack:      ack,
		}

		select {
		case out <- ingestMsg:
		case <-ctx.Done():
			return
		}

		// Wait for write confirmation before acknowledging to RELP sender.
		select {
		case writeErr := <-ack:
			if writeErr != nil {
				if err := session.AnswerError(msg, writeErr.Error()); err != nil {
					r.logger.Debug("RELP answer error failed", "error", err)
					return
				}
			} else {
				if err := session.AnswerOk(msg); err != nil {
					r.logger.Debug("RELP answer ok failed", "error", err)
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
