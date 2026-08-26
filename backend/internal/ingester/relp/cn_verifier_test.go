package relp

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestBuildCNVerifier covers the CN-ACL decision in isolation: allowed,
// rejected, and no-certificate-presented.
func TestBuildCNVerifier(t *testing.T) {
	t.Parallel()

	allowedCert := &x509.Certificate{Subject: pkix.Name{CommonName: "worker-3.example.com"}}

	cases := []struct {
		name    string
		pattern string
		certs   []*x509.Certificate
		wantErr bool
	}{
		{"CN matches the wildcard", "worker-*.example.com", []*x509.Certificate{allowedCert}, false},
		{"CN not covered by the pattern", "admin-*.example.com", []*x509.Certificate{allowedCert}, true},
		{"no client certificate presented", "worker-*.example.com", nil, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			verify := buildCNVerifier(tc.pattern)
			err := verify(tls.ConnectionState{PeerCertificates: tc.certs})
			if tc.wantErr && err == nil {
				t.Fatalf("expected the CN check to reject, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected the CN check to accept, got: %v", err)
			}
		})
	}
}

// testCA holds a self-signed CA usable to mint server and client leaf certs
// for mTLS tests.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return &testCA{cert: caCert, key: key, pool: pool}
}

// leafCert mints a leaf certificate signed by the CA. serverAuth selects
// server- vs client-auth extended key usage.
func (ca *testCA) leafCert(t *testing.T, commonName string, serial int64, serverAuth bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if serverAuth {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{commonName}
		tmpl.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load leaf keypair: %v", err)
	}
	return pair
}

// verifyCall records one VerifyConnection invocation as observed by the
// server, including whether the connection it fired on was resumed.
type verifyCall struct {
	didResume bool
	err       error
}

// recordingVerifier wraps a VerifyConnection callback to record every
// invocation, so a test can assert both the call count and what each call
// observed (in particular, ConnectionState.DidResume).
func recordingVerifier(inner func(tls.ConnectionState) error) (verify func(tls.ConnectionState) error, calls func() []verifyCall) {
	var mu sync.Mutex
	var recorded []verifyCall
	verify = func(cs tls.ConnectionState) error {
		err := inner(cs)
		mu.Lock()
		recorded = append(recorded, verifyCall{didResume: cs.DidResume, err: err})
		mu.Unlock()
		return err
	}
	calls = func() []verifyCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]verifyCall(nil), recorded...)
	}
	return verify, calls
}

// acceptOnce runs a single TLS server handshake on the next accepted
// connection, then writes one byte and closes — enough for the client to
// receive a TLS 1.3 post-handshake session ticket via its next Read.
func acceptOnce(t *testing.T, ln net.Listener, errCh chan<- error) {
	t.Helper()
	conn, err := ln.Accept()
	if err != nil {
		errCh <- err
		return
	}
	tconn, ok := conn.(*tls.Conn)
	if !ok {
		errCh <- errors.New("accepted connection is not a *tls.Conn")
		return
	}
	if err := tconn.Handshake(); err != nil {
		errCh <- err
		_ = tconn.Close()
		return
	}
	_, _ = tconn.Write([]byte{1})
	_ = tconn.Close()
	errCh <- nil
}

// dialAndPumpTicket dials the server and reads one byte back so any TLS 1.3
// post-handshake session ticket the server sent is processed into the
// client's session cache before the connection closes.
func dialAndPumpTicket(t *testing.T, addr string, clientCfg *tls.Config) (*tls.ConnectionState, error) {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cs := conn.ConnectionState()
	buf := make([]byte, 1)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Read(buf)
	return &cs, nil
}

// TestBuildCNVerifier_RunsOnResumedSessions proves the regression class a
// revert to VerifyPeerCertificate would reopen: VerifyConnection (unlike
// VerifyPeerCertificate) fires again on a resumed TLS session, so an ACL
// tightened between connections is still enforced even when the client
// presents a still-valid session ticket instead of doing a full handshake.
func TestBuildCNVerifier_RunsOnResumedSessions(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	serverCert := ca.leafCert(t, "127.0.0.1", 2, true)
	clientCert := ca.leafCert(t, "worker-3.example.com", 3, false)

	var mu sync.Mutex
	verify, calls := recordingVerifier(buildCNVerifier("worker-*.example.com"))

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    ca.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			mu.Lock()
			defer mu.Unlock()
			return verify(cs)
		},
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	clientCfg := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            ca.pool,
		ServerName:         "127.0.0.1",
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
		MinVersion:         tls.VersionTLS13,
	}

	// First connection: full handshake, ACL allows the client's CN.
	errCh := make(chan error, 1)
	go acceptOnce(t, ln, errCh)
	cs1, err := dialAndPumpTicket(t, ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("first server handshake: %v", err)
	}
	if cs1.DidResume {
		t.Fatal("first connection unexpectedly resumed a session")
	}

	// Second connection: same client, same session cache. If a session
	// ticket was cached, this resumes rather than doing a full handshake.
	go acceptOnce(t, ln, errCh)
	cs2, err := dialAndPumpTicket(t, ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("second server handshake: %v", err)
	}
	if !cs2.DidResume {
		t.Skip("session was not resumed (no ticket observed in time); resumption timing is environment-dependent")
	}

	got := calls()
	if len(got) != 2 {
		t.Fatalf("expected the verifier to run twice, ran %d times", len(got))
	}
	if got[0].didResume {
		t.Error("first invocation should not have observed a resumed session")
	}
	if got[0].err != nil {
		t.Errorf("first invocation should have accepted the allowed CN, got: %v", got[0].err)
	}
	if !got[1].didResume {
		t.Error("second invocation should have observed a resumed session")
	}
	if got[1].err != nil {
		t.Errorf("second invocation should have accepted the allowed CN, got: %v", got[1].err)
	}
}

// TestBuildCNVerifier_EnforcesTightenedACLOnResumedSession is the negative
// twin: the ACL is tightened between the two connections (as if an operator
// edited tls_allowed_cn), and the resumed second connection must still be
// rejected. Under VerifyPeerCertificate, a resumed handshake never re-runs
// the callback, so the client would keep talking on its old session ticket
// even after the CN it was validated for lost access — VerifyConnection is
// what closes that gap.
func TestBuildCNVerifier_EnforcesTightenedACLOnResumedSession(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	serverCert := ca.leafCert(t, "127.0.0.1", 2, true)
	clientCert := ca.leafCert(t, "worker-3.example.com", 3, false)

	var mu sync.Mutex
	verify, calls := recordingVerifier(buildCNVerifier("worker-*.example.com"))

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    ca.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			mu.Lock()
			defer mu.Unlock()
			return verify(cs)
		},
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	clientCfg := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            ca.pool,
		ServerName:         "127.0.0.1",
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
		MinVersion:         tls.VersionTLS13,
	}

	errCh := make(chan error, 1)
	go acceptOnce(t, ln, errCh)
	cs1, err := dialAndPumpTicket(t, ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("first server handshake: %v", err)
	}
	if cs1.DidResume {
		t.Fatal("first connection unexpectedly resumed a session")
	}

	// Tighten the ACL: the previously-allowed CN is no longer covered.
	// serverCfg.VerifyConnection closes over `verify` by reference, so
	// reassigning it here changes what the next handshake observes without
	// needing to touch the config itself.
	mu.Lock()
	verify, calls = recordingVerifier(buildCNVerifier("admin-*.example.com"))
	mu.Unlock()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		tconn := conn.(*tls.Conn)
		err = tconn.Handshake()
		_ = tconn.Close()
		errCh <- err
	}()

	// For a resumed handshake, the server sends its Finished flight before
	// readClientCertificate runs VerifyConnection (it doesn't re-request a
	// client certificate on resumption), so a rejection there lands after
	// the client has already committed to the handshake. tls.Dial itself
	// can therefore return successfully; the rejection surfaces as the
	// server closing/alerting instead of completing the exchange, which is
	// exactly what a client attempting to use the connection observes.
	conn2, dialErr := tls.Dial("tcp", ln.Addr().String(), clientCfg)
	var exchangeErr error
	if dialErr == nil {
		defer conn2.Close()
		_, exchangeErr = conn2.Write([]byte{0})
		if exchangeErr == nil {
			buf := make([]byte, 1)
			_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, exchangeErr = conn2.Read(buf)
		}
	}
	if dialErr == nil && exchangeErr == nil {
		t.Fatal("expected the resumed connection to be rejected by the tightened ACL, but it was usable")
	}

	if serverErr := <-errCh; serverErr == nil {
		t.Fatal("expected the server handshake to fail too")
	}

	got := calls()
	if len(got) != 1 {
		t.Fatalf("expected the tightened verifier to run once, ran %d times", len(got))
	}
	if !got[0].didResume {
		t.Skip("session was not resumed (no ticket observed in time); resumption timing is environment-dependent")
	}
	if got[0].err == nil {
		t.Error("expected the resumed connection to be rejected under the tightened ACL")
	}
	if !strings.Contains(got[0].err.Error(), "does not match allowed pattern") {
		t.Errorf("expected a CN-mismatch error, got: %v", got[0].err)
	}
}
