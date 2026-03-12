// Package relp provides a RELP (Reliable Event Logging Protocol) ingester.
// RELP is a TCP-based reliable syslog transport with transaction-based
// acknowledgments, commonly used by rsyslog.
package relp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gastrolog/internal/cert"
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

	tlsConfig *tls.Config

	mu       sync.Mutex
	listener net.Listener
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
		id:        cfg.ID,
		addr:      cfg.Addr,
		tlsConfig: cfg.TLSConfig,
		logger:    logging.Default(cfg.Logger).With("component", "ingester", "type", "relp"),
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

		wg.Go(func() {
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
func (r *Ingester) handleConn(ctx context.Context, conn net.Conn, out chan<- orchestrator.IngestMessage) {
	defer func() { _ = conn.Close() }()

	remoteIP := ""
	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = tcpAddr.IP.String()
	}

	// Wrap with TLS if configured.
	var fd io.ReadWriter = conn
	if r.tlsConfig != nil {
		tlsConn := tls.Server(conn, r.tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			r.logger.Debug("RELP TLS handshake failed", "error", err, "remote", remoteIP)
			return
		}
		fd = tlsConn
	}

	session := NewSession(fd, fd)

	r.logger.Debug("RELP session established", "remote", remoteIP)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := session.ReceiveLog()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				r.logger.Debug("RELP receive ended", "error", err, "remote", remoteIP)
			}
			return
		}

		attrs, sourceTS := syslogparse.ParseMessage(msg.Data, remoteIP)
		attrs["ingester_type"] = "relp"

		// Use ack channel for end-to-end delivery guarantee:
		// the orchestrator sends nil/error after writing to chunk store.
		ack := make(chan error, 1)

		ingestMsg := orchestrator.IngestMessage{
			Attrs:      attrs,
			Raw:        msg.Data,
			SourceTS:   sourceTS,
			IngestTS:   time.Now(),
			IngesterID: r.id,
			Ack:        ack,
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

// BuildTLSConfig builds a *tls.Config from ingester parameters.
// Returns nil if TLS is not configured (tls param is empty or "false").
//
// The server certificate is resolved from the cert manager by name
// (tls_cert param). For mutual TLS, tls_ca specifies the CA file path
// and tls_allowed_cn optionally restricts client certificate CNs.
func BuildTLSConfig(params map[string]string, certMgr *cert.Manager) (*tls.Config, error) {
	if params["tls"] != "true" {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Resolve server certificate from the cert manager by name.
	// Uses GetCertificate callback so cert rotations are picked up automatically.
	certName := params["tls_cert"]
	if certName != "" {
		if certMgr == nil {
			return nil, errors.New("RELP TLS: cert manager not available")
		}
		// Verify the cert exists at config time.
		if certMgr.Certificate(certName) == nil {
			return nil, fmt.Errorf("RELP TLS: certificate %q not found in cert manager", certName)
		}
		cfg.GetCertificate = func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
			c := certMgr.Certificate(certName)
			if c == nil {
				return nil, fmt.Errorf("RELP TLS: certificate %q no longer available", certName)
			}
			return c, nil
		}
	}

	// Load CA for client certificate verification (mutual TLS).
	caFile := params["tls_ca"]
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile) //nolint:gosec // G304: CA file path from user config
		if err != nil {
			return nil, fmt.Errorf("read RELP CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("RELP CA file contains no valid certificates")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert

		// Optional CN-based ACL.
		if pattern := params["tls_allowed_cn"]; pattern != "" {
			cfg.VerifyPeerCertificate = buildCNVerifier(pattern)
		}
	}

	return cfg, nil
}

// buildCNVerifier returns a VerifyPeerCertificate function that checks
// the client certificate's Common Name against a wildcard pattern.
func buildCNVerifier(pattern string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("relp: no client certificate provided")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("relp: parse client certificate: %w", err)
		}
		matched, err := filepath.Match(pattern, cert.Subject.CommonName)
		if err != nil {
			return fmt.Errorf("relp: invalid CN pattern %q: %w", pattern, err)
		}
		if !matched {
			return fmt.Errorf("relp: client CN %q does not match allowed pattern %q", cert.Subject.CommonName, pattern)
		}
		return nil
	}
}
