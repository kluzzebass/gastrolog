package server

// The HTTPS redirect builds its target from the Host header, which is the
// client's. Reflecting it sends the caller wherever they asked, under this
// server's name. The certificates this server can present are the record of
// which names are actually its own.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/cert"
)

// certManagerFor builds a cert manager holding one self-signed certificate
// valid for the given names.
func certManagerFor(t *testing.T, names ...string) *cert.Manager {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: names[0]},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     names,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	m := cert.New(cert.Config{})
	if err := m.AddFromPEM("default", string(certPEM), string(keyPEM)); err != nil {
		t.Fatal(err)
	}
	m.SetDefault("default")
	return m
}

func redirectingServer(t *testing.T, names ...string) http.Handler {
	t.Helper()
	s := &Server{certManager: certManagerFor(t, names...), httpsPort: "8443"}
	s.redirectToHTTPS.Store(true)
	return s.redirectMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func TestHTTPSRedirectOnlyToHostsWeHoldACertificateFor(t *testing.T) {
	t.Parallel()
	handler := redirectingServer(t, "logs.example.com")

	t.Run("a name we hold a certificate for is redirected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui", nil)
		req.Host = "logs.example.com"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusTemporaryRedirect {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusTemporaryRedirect)
		}
		if got := rec.Header().Get("Location"); !strings.HasPrefix(got, "https://logs.example.com:8443/") {
			t.Errorf("Location = %q, want the certificate's own host", got)
		}
	})

	t.Run("a name we do not is served, not redirected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui", nil)
		req.Host = "attacker.example.net"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code == http.StatusTemporaryRedirect {
			t.Fatalf("redirected to %q, a host this server holds no certificate for",
				rec.Header().Get("Location"))
		}
		if loc := rec.Header().Get("Location"); strings.Contains(loc, "attacker.example.net") {
			t.Errorf("Location = %q reflects the client's Host header", loc)
		}
	})
}

// With no certificates at all there is no name to vouch for, so nothing is
// redirected rather than everything being.
func TestHTTPSRedirectWithoutCertificatesRedirectsNothing(t *testing.T) {
	t.Parallel()
	s := &Server{httpsPort: "8443"}
	s.redirectToHTTPS.Store(true)
	handler := s.redirectMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/ui", nil)
	req.Host = "logs.example.com"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code == http.StatusTemporaryRedirect {
		t.Fatalf("redirected to %q with no certificate to present there",
			rec.Header().Get("Location"))
	}
}

// A certificate valid for the requested name is the whole test, so a
// manager holding one for a different name must not vouch for it.
func TestHoldsCertificateForChecksTheName(t *testing.T) {
	t.Parallel()
	s := &Server{certManager: certManagerFor(t, "logs.example.com")}

	if !s.holdsCertificateFor("logs.example.com") {
		t.Error("the certificate's own name was not recognised")
	}
	if s.holdsCertificateFor("other.example.com") {
		t.Error("a name the certificate does not cover was accepted")
	}
}
