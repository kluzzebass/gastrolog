// Package ingesttls builds TLS configurations from ingester parameters, one
// vocabulary across every ingester that speaks TLS:
//
//	tls             "true" enables TLS (listeners serve it, clients dial it)
//	tls_cert        certificate name in the cert store — the listener's
//	                serving pair, or the client's identity for mutual TLS
//	tls_ca          certificate name in the cert store whose chain becomes
//	                the trust root — client verification on listeners
//	                (presence demands a client certificate), server
//	                verification on clients (overrides the system roots)
//	tls_allowed_cn  listeners only: wildcard pattern a client certificate's
//	                CN must match
//	tls_verify      clients only: "false" disables server verification.
//	                Unsafe; the caller logs a warning. tls_ca covers the
//	                self-signed case without giving up verification.
//
// Certificates are resolved from the cert manager by name on every
// handshake, so rotations are picked up without a restart.
package ingesttls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"path/filepath"

	"gastrolog/internal/cert"
)

// Server builds a listener-side *tls.Config from ingester parameters.
// Returns (nil, nil) when the tls param is not "true". name prefixes errors
// so a misconfiguration names the ingester that carries it.
func Server(name string, params map[string]string, certMgr *cert.Manager) (*tls.Config, error) {
	if params["tls"] != "true" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	certName := params["tls_cert"]
	if certName != "" {
		getCert, err := namedCertificate(name, certName, certMgr)
		if err != nil {
			return nil, err
		}
		cfg.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return getCert()
		}
	}

	if caName := params["tls_ca"]; caName != "" {
		pool, err := namedPool(name, caName, certMgr)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		if pattern := params["tls_allowed_cn"]; pattern != "" {
			// VerifyConnection instead of VerifyPeerCertificate: it runs on
			// every handshake, including a resumed one, so the CN check
			// cannot be skipped by session resumption.
			cfg.VerifyConnection = cnVerifier(name, pattern)
		}
	}
	return cfg, nil
}

// Client builds a dial-side *tls.Config from ingester parameters. Returns
// (nil, nil) when the tls param is not "true". The returned bool reports
// whether server verification was disabled (tls_verify=false), so the
// caller can log the warning the setting deserves.
func Client(name string, params map[string]string, certMgr *cert.Manager) (cfg *tls.Config, insecure bool, err error) {
	if params["tls"] != "true" {
		return nil, false, nil
	}
	cfg = &tls.Config{MinVersion: tls.VersionTLS12}

	if caName := params["tls_ca"]; caName != "" {
		pool, err := namedPool(name, caName, certMgr)
		if err != nil {
			return nil, false, err
		}
		cfg.RootCAs = pool
	}

	if certName := params["tls_cert"]; certName != "" {
		getCert, err := namedCertificate(name, certName, certMgr)
		if err != nil {
			return nil, false, err
		}
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return getCert()
		}
	}

	if params["tls_verify"] == "false" {
		cfg.InsecureSkipVerify = true // explicit operator opt-in; the caller logs it
		insecure = true
	}
	return cfg, insecure, nil
}

// namedCertificate resolves a certificate from the manager, verifying it
// exists at config time and re-resolving on every handshake for rotation.
func namedCertificate(name, certName string, certMgr *cert.Manager) (func() (*tls.Certificate, error), error) {
	if certMgr == nil {
		return nil, fmt.Errorf("%s TLS: cert manager not available", name)
	}
	if certMgr.Certificate(certName) == nil {
		return nil, fmt.Errorf("%s TLS: certificate %q not found in cert manager", name, certName)
	}
	return func() (*tls.Certificate, error) {
		c := certMgr.Certificate(certName)
		if c == nil {
			return nil, fmt.Errorf("%s TLS: certificate %q no longer available", name, certName)
		}
		return c, nil
	}, nil
}

// namedPool builds an x509 pool from the named certificate's chain.
func namedPool(name, caName string, certMgr *cert.Manager) (*x509.CertPool, error) {
	if certMgr == nil {
		return nil, fmt.Errorf("%s TLS: cert manager not available", name)
	}
	c := certMgr.Certificate(caName)
	if c == nil {
		return nil, fmt.Errorf("%s TLS: CA certificate %q not found in cert manager", name, caName)
	}
	pool := x509.NewCertPool()
	for _, der := range c.Certificate {
		parsed, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("%s TLS: parse CA certificate %q: %w", name, caName, err)
		}
		pool.AddCert(parsed)
	}
	return pool, nil
}

// cnVerifier returns a VerifyConnection function that checks the client
// certificate's Common Name against a wildcard pattern.
func cnVerifier(name, pattern string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("%s TLS: no client certificate provided", name)
		}
		cn := cs.PeerCertificates[0].Subject.CommonName
		matched, err := filepath.Match(pattern, cn)
		if err != nil {
			return fmt.Errorf("%s TLS: invalid CN pattern %q: %w", name, pattern, err)
		}
		if !matched {
			return fmt.Errorf("%s TLS: client CN %q does not match %q", name, cn, pattern)
		}
		return nil
	}
}
