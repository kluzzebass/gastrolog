package ingesttls

// One TLS vocabulary across every ingester means one place to prove it: the
// happy handshakes, and the adversarial ones — a client the CA did not sign,
// a CN the ACL does not allow — refused at the transport, not downstream.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/cert"
)

// pemPair is a PEM-encoded certificate and key.
type pemPair struct{ certPEM, keyPEM []byte }

// testCA mints a CA; testLeaf mints a leaf signed by it with the given CN
// and SANs. Self-contained so these tests pin the helper's behavior, not a
// certificate library's API surface.
func testCA(t *testing.T, cn string) (pemPair, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pemPair{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, parsed, key
}

func testLeaf(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, sans []string) pemPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     sans,
	}
	for _, s := range sans {
		if ip := net.ParseIP(s); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pemPair{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// managerWith loads named cert pairs into a cert manager.
func managerWith(t *testing.T, pairs map[string]pemPair) *cert.Manager {
	t.Helper()
	m := cert.New(cert.Config{})
	for name, p := range pairs {
		if err := m.AddFromPEM(name, string(p.certPEM), string(p.keyPEM)); err != nil {
			t.Fatalf("AddFromPEM(%s): %v", name, err)
		}
	}
	return m
}

// caPool builds a pool from a CA PEM.
func caPool(t *testing.T, caPEM []byte) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA PEM contains no certificates")
	}
	return pool
}

// fixture returns a manager holding a CA ("ca"), a server pair signed by it
// ("srv", SAN localhost), and a client pair signed by it ("cli", CN
// client-1), plus the CA signing material for minting adversaries.
func fixture(t *testing.T) (*cert.Manager, pemPair, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	caPair, caCert, caKey := testCA(t, "test-ca")
	srv := testLeaf(t, caCert, caKey, "srv-node", []string{"localhost", "127.0.0.1"})
	cli := testLeaf(t, caCert, caKey, "client-1", nil)
	return managerWith(t, map[string]pemPair{"ca": caPair, "srv": srv, "cli": cli}), caPair, caCert, caKey
}

// handshake dials a one-shot TLS listener built from serverCfg with
// clientCfg and reports both sides' errors.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) (serverErr, clientErr error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		s := tls.Server(conn, serverCfg)
		done <- s.Handshake()
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), clientCfg)
	if err == nil {
		_ = conn.Close()
	}
	return <-done, err
}

func TestServerOffWhenParamAbsent(t *testing.T) {
	t.Parallel()
	cfg, err := Server("x", map[string]string{}, nil)
	if cfg != nil || err != nil {
		t.Fatalf("want (nil, nil) for tls off, got (%v, %v)", cfg, err)
	}
}

func TestServerServesNamedCertificate(t *testing.T) {
	t.Parallel()
	mgr, caPair, _, _ := fixture(t)
	srvCfg, err := Server("x", map[string]string{"tls": "true", "tls_cert": "srv"}, mgr)
	if err != nil {
		t.Fatal(err)
	}

	pool := caPool(t, caPair.certPEM)
	sErr, cErr := handshake(t, srvCfg, &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	if sErr != nil || cErr != nil {
		t.Fatalf("handshake failed: server=%v client=%v", sErr, cErr)
	}
}

// A listener with tls_ca demands a client certificate signed by that CA; a
// client without one, or signed by a different CA, is refused at the
// transport — forged records never reach the parser.
func TestServerRefusesClientTheCADidNotSign(t *testing.T) {
	t.Parallel()
	mgr, caPair, _, _ := fixture(t)
	srvCfg, err := Server("x", map[string]string{"tls": "true", "tls_cert": "srv", "tls_ca": "ca"}, mgr)
	if err != nil {
		t.Fatal(err)
	}
	pool := caPool(t, caPair.certPEM)

	// No client certificate at all.
	sErr, _ := handshake(t, srvCfg, &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	if sErr == nil {
		t.Fatal("a client with no certificate completed the handshake")
	}

	// A certificate from a different CA.
	_, otherCert, otherKey := testCA(t, "foreign-ca")
	forged := testLeaf(t, otherCert, otherKey, "client-1", nil)
	forgedCert, err := tls.X509KeyPair(forged.certPEM, forged.keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	sErr, _ = handshake(t, srvCfg, &tls.Config{
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{forgedCert},
	})
	if sErr == nil {
		t.Fatal("a client certificate from a foreign CA completed the handshake")
	}
}

// The CN ACL admits matching clients and refuses the rest, however valid
// their certificate chain.
func TestServerCNACL(t *testing.T) {
	t.Parallel()
	mgr, caPair, _, _ := fixture(t)
	pool := caPool(t, caPair.certPEM)
	cliCert := mgr.Certificate("cli")

	srvCfg, err := Server("x", map[string]string{
		"tls": "true", "tls_cert": "srv", "tls_ca": "ca", "tls_allowed_cn": "client-*",
	}, mgr)
	if err != nil {
		t.Fatal(err)
	}
	sErr, cErr := handshake(t, srvCfg, &tls.Config{
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{*cliCert},
	})
	if sErr != nil || cErr != nil {
		t.Fatalf("matching CN refused: server=%v client=%v", sErr, cErr)
	}

	srvCfg, err = Server("x", map[string]string{
		"tls": "true", "tls_cert": "srv", "tls_ca": "ca", "tls_allowed_cn": "producer-*",
	}, mgr)
	if err != nil {
		t.Fatal(err)
	}
	sErr, _ = handshake(t, srvCfg, &tls.Config{
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{*cliCert},
	})
	if sErr == nil || !strings.Contains(sErr.Error(), "does not match") {
		t.Fatalf("CN outside the ACL completed the handshake: %v", sErr)
	}
}

func TestServerUnknownCertificateNameFailsAtConfigTime(t *testing.T) {
	t.Parallel()
	mgr, _, _, _ := fixture(t)
	if _, err := Server("x", map[string]string{"tls": "true", "tls_cert": "missing"}, mgr); err == nil {
		t.Fatal("an unknown certificate name was accepted")
	}
	if _, err := Server("x", map[string]string{"tls": "true", "tls_ca": "missing"}, mgr); err == nil {
		t.Fatal("an unknown CA name was accepted")
	}
	if _, err := Server("x", map[string]string{"tls": "true", "tls_cert": "srv"}, nil); err == nil {
		t.Fatal("a nil cert manager was accepted alongside a named certificate")
	}
}

// Client TLS: tls_ca pins the trust root, so a server the named CA did not
// sign is refused — the MITM case tls_verify=false would invite.
func TestClientRefusesServerTheCADidNotSign(t *testing.T) {
	t.Parallel()
	mgr, _, _, _ := fixture(t)

	_, otherCert, otherKey := testCA(t, "mitm-ca")
	mitm := testLeaf(t, otherCert, otherKey, "srv-node", []string{"localhost", "127.0.0.1"})
	mitmCert, err := tls.X509KeyPair(mitm.certPEM, mitm.keyPEM)
	if err != nil {
		t.Fatal(err)
	}

	cliCfg, insecure, err := Client("x", map[string]string{"tls": "true", "tls_ca": "ca"}, mgr)
	if err != nil || insecure {
		t.Fatalf("client config: err=%v insecure=%v", err, insecure)
	}
	cliCfg.ServerName = "localhost"
	_, cErr := handshake(t, &tls.Config{Certificates: []tls.Certificate{mitmCert}, MinVersion: tls.VersionTLS12}, cliCfg)
	if cErr == nil {
		t.Fatal("a server certificate from a foreign CA was accepted")
	}

	// And tls_verify=false admits it — which is why the flag is reported
	// for the caller to warn about.
	cliCfg, insecure, err = Client("x", map[string]string{"tls": "true", "tls_verify": "false"}, mgr)
	if err != nil || !insecure {
		t.Fatalf("client config: err=%v insecure=%v, want insecure reported", err, insecure)
	}
	sErr, cErr := handshake(t, &tls.Config{Certificates: []tls.Certificate{mitmCert}, MinVersion: tls.VersionTLS12}, cliCfg)
	if sErr != nil || cErr != nil {
		t.Fatalf("tls_verify=false still refused the handshake: server=%v client=%v", sErr, cErr)
	}
}

// Rotation: the certificate is re-resolved per handshake, so replacing it in
// the manager takes effect without a restart.
func TestServerPicksUpRotatedCertificate(t *testing.T) {
	t.Parallel()
	mgr, caPair, caCert, caKey := fixture(t)
	srvCfg, err := Server("x", map[string]string{"tls": "true", "tls_cert": "srv"}, mgr)
	if err != nil {
		t.Fatal(err)
	}

	rotated := testLeaf(t, caCert, caKey, "srv-node-rotated", []string{"localhost", "127.0.0.1"})
	if err := mgr.AddFromPEM("srv", string(rotated.certPEM), string(rotated.keyPEM)); err != nil {
		t.Fatal(err)
	}

	pool := caPool(t, caPair.certPEM)
	seen := ""
	cliCfg := &tls.Config{
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12,
		VerifyConnection: func(cs tls.ConnectionState) error {
			seen = cs.PeerCertificates[0].Subject.CommonName
			return nil
		},
	}
	if sErr, cErr := handshake(t, srvCfg, cliCfg); sErr != nil || cErr != nil {
		t.Fatalf("handshake after rotation: server=%v client=%v", sErr, cErr)
	}
	if seen != "srv-node-rotated" {
		t.Fatalf("server presented %q, want the rotated certificate", seen)
	}
}
