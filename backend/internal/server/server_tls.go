package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"

	"connectrpc.com/connect"

	"gastrolog/internal/auth"
)

// reconfigureTLS starts/stops HTTPS listener based on system. Safe to call from any goroutine.
func (s *Server) reconfigureTLS() {
	ctx, cancel := context.WithTimeout(context.Background(), systemLoadTimeout)
	defer cancel()
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		s.logger.Warn("reconfigure TLS: load config failed", "error", err)
		return
	}
	// Fall back to HTTP if no default cert or TLS disabled
	tlsEnabled := ss.TLS.TLSEnabled && ss.TLS.DefaultCert != ""
	redirectEnabled := ss.TLS.HTTPToHTTPSRedirect && tlsEnabled

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update redirect state
	s.redirectToHTTPS.Store(redirectEnabled)

	if !tlsEnabled {
		s.stopHTTPSLocked()
		return
	}

	if s.certManager == nil {
		s.stopHTTPSLocked()
		return
	}

	// HTTPS port: use configured value, or derive from HTTP listener port + 1
	httpsPort := ss.TLS.HTTPSPort
	if httpsPort == "" {
		httpsPort = s.deriveHTTPSPort()
	}
	if httpsPort == "" {
		s.logger.Warn("reconfigure TLS: cannot determine HTTPS port")
		return
	}
	s.httpsPort = httpsPort

	// Already running?
	if s.httpsListener != nil {
		return
	}

	// Start HTTPS listener
	httpsAddr := ":" + httpsPort
	ln, err := net.Listen("tcp", httpsAddr)
	if err != nil {
		s.logger.Warn("reconfigure TLS: listen failed", "addr", httpsAddr, "error", err)
		return
	}
	tlsConfig := s.certManager.TLSConfig()
	// Harden server-side TLS: require TLS 1.2+ and prefer modern curves.
	// CertManager.TLSConfig() is generic (also used for client certs);
	// server hardening is applied here, not in the shared system.
	tlsConfig.MinVersion = tls.VersionTLS12
	tlsConfig.CurvePreferences = []tls.CurveID{tls.X25519, tls.CurveP256}
	tlsLn := tls.NewListener(ln, tlsConfig)

	s.httpsListener = tlsLn
	s.httpsServer = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	s.logger.Info("HTTPS listener started", "addr", httpsAddr)

	go func() {
		if err := s.httpsServer.Serve(tlsLn); err != nil && err != http.ErrServerClosed {
			s.logger.Warn("HTTPS serve error", "error", err)
		}
	}()
}

func (s *Server) deriveHTTPSPort() string {
	if s.listener == nil {
		return ""
	}
	addr := s.listener.Addr().String()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return ""
	}
	return strconv.Itoa(port + 1)
}

func (s *Server) stopHTTPSLocked() {
	if s.httpsServer != nil {
		_ = s.httpsServer.Shutdown(context.Background())
		s.httpsServer = nil
	}
	if s.httpsListener != nil {
		_ = s.httpsListener.Close()
		s.httpsListener = nil
	}
	s.httpsPort = ""
}

// ListenUnix starts a secondary Unix socket listener alongside the primary
// TCP listener. Requests over the socket bypass authentication, providing
// token-free access for the local CLI. The socket file is removed on Stop.
// Must be called after Serve has set up the handler.
func (s *Server) ListenUnix(path string) error {
	// Remove stale socket file from a previous unclean shutdown.
	_ = os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", path, err)
	}
	// Restrict socket to owner only.
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return fmt.Errorf("chmod unix socket: %w", err)
	}

	// Build a separate mux with NoAuthInterceptor so the Connect layer
	// skips JWT validation entirely. The OS file permissions on the socket
	// provide the access control.
	noAuthOpt := connect.WithInterceptors(&auth.NoAuthInterceptor{})
	mux := s.buildMux(noAuthOpt)
	handler := s.trackingMiddleware(s.corsMiddleware(securityHeadersMiddleware(rateLimitMiddleware(s.rl)(compressMiddleware(mux)))))

	s.mu.Lock()
	s.unixListener = ln
	s.unixPath = path
	s.unixServer = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	s.mu.Unlock()

	s.logger.Info("unix socket listener started", "path", path)

	go func() {
		if err := s.unixServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.logger.Warn("unix socket serve error", "error", err)
		}
	}()
	return nil
}
