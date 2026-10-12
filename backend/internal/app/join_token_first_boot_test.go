package app

// A join token buys one thing: the certificate a node that has none enrols
// for. A node whose home already holds its certificate starts from it and
// never contacts a token source — a whole-cluster restart must not wait on
// the token-serving node's HTTP listener, which opens only once Raft has
// quorum. Everything short of a loadable certificate still goes through the
// token.

import (
	"context"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

const tokenSourceSecret = "shared-serve-secret"

// tokenSource is a /cluster/bootstrap-token endpoint that counts every
// request it receives, so a test can assert a node never asked.
type tokenSource struct {
	srv  *httptest.Server
	hits atomic.Int32
}

func newTokenSource(t *testing.T, mint func() (string, error)) *tokenSource {
	t.Helper()
	ts := &tokenSource{}
	ts.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts.hits.Add(1)
		if r.Header.Get(bootstrapTokenSecretHeader) != tokenSourceSecret {
			http.Error(w, "bad secret", http.StatusUnauthorized)
			return
		}
		token, err := mint()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, token)
	}))
	t.Cleanup(ts.srv.Close)
	return ts
}

func (ts *tokenSource) url() string { return ts.srv.URL + "/cluster/bootstrap-token" }

// enrollingMember is a cluster member that answers Enroll out of replicated
// cluster TLS, the way every member does.
type enrollingMember struct {
	addr string
	ca   tlsutil.CAKeyPair
	mint func() (string, error)
}

func newEnrollingMember(t *testing.T) *enrollingMember {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	key, err := tlsutil.GenerateJoinTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	store := sysmem.NewStore()
	if err := store.PutClusterTLS(context.Background(), system.ClusterTLS{
		CACertPEM:    string(ca.CertPEM),
		CAKeyPEM:     string(ca.KeyPEM),
		JoinTokenKey: hex.EncodeToString(key),
	}); err != nil {
		t.Fatal(err)
	}

	id := glid.New().String()
	cert, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, id, cluster.LaneSANs)
	if err != nil {
		t.Fatal(err)
	}
	ctls := cluster.NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatal(err)
	}
	srv, err := cluster.New(cluster.Config{ClusterAddr: "127.0.0.1:0", NodeID: id, TLS: ctls, Logger: discardLogger()})
	if err != nil {
		t.Fatal(err)
	}
	srv.SetEnrollHandler(makeEnrollHandler(store, nil, discardLogger()))
	_ = srv.Transport() // Start registers the Raft lanes on it
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	return &enrollingMember{addr: srv.Addr(), ca: ca, mint: makeBootstrapTokenFn(store)}
}

// joinerConfig is what an unattended joiner is started with on every boot.
func joinerConfig(joinAddr, tokenURL string) RunConfig {
	return RunConfig{
		ConfigType:           "raft",
		ClusterAddr:          "127.0.0.1:0",
		JoinAddr:             joinAddr,
		BootstrapTokenURL:    tokenURL,
		BootstrapTokenSecret: tokenSourceSecret,
	}
}

// bootCluster runs setupCluster with a bounded context and stops the server
// it creates when the test ends.
func bootCluster(t *testing.T, cfg RunConfig, hd home.Dir, nodeID string, within time.Duration) (*cluster.ClusterTLS, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	srv, ctls, err := setupCluster(ctx, discardLogger(), cfg, hd, nodeID)
	if err != nil {
		return nil, err
	}
	t.Cleanup(srv.Stop)
	return ctls, nil
}

// requireCertFrom asserts ctls holds a certificate for nodeID issued by ca.
func requireCertFrom(t *testing.T, ctls *cluster.ClusterTLS, ca tlsutil.CAKeyPair, nodeID string) {
	t.Helper()
	st := ctls.State()
	if st == nil {
		t.Fatal("node holds no cluster certificate")
	}
	leaf, err := x509.ParseCertificate(st.Cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if got := tlsutil.NodeIDFromCert(leaf); got != nodeID {
		t.Fatalf("certificate names %q, want %q", got, nodeID)
	}
	if !ctls.HasCA(ca.CertPEM) {
		t.Fatal("certificate is not under the member's cluster CA")
	}
}

func TestFreshJoinerFetchesTheTokenAndARestartNeverAsksAgain(t *testing.T) {
	t.Parallel()
	member := newEnrollingMember(t)
	source := newTokenSource(t, member.mint)
	hd := home.New(t.TempDir())
	nodeID := glid.New().String()
	cfg := joinerConfig(member.addr, source.url())

	first, err := bootCluster(t, cfg, hd, nodeID, 30*time.Second)
	if err != nil {
		t.Fatalf("first boot: %v", err)
	}
	if got := source.hits.Load(); got != 1 {
		t.Fatalf("first boot asked the token source %d times, want 1", got)
	}
	requireCertFrom(t, first, member.ca, nodeID)

	for boot := 2; boot <= 3; boot++ {
		again, err := bootCluster(t, cfg, hd, nodeID, 30*time.Second)
		if err != nil {
			t.Fatalf("boot %d: %v", boot, err)
		}
		if got := source.hits.Load(); got != 1 {
			t.Fatalf("boot %d asked the token source; %d requests in total, want the first boot's 1", boot, got)
		}
		if string(again.State().Cert.Certificate[0]) != string(first.State().Cert.Certificate[0]) {
			t.Fatalf("boot %d holds a different certificate than the one enrolment issued", boot)
		}
	}
}

func TestRestartingJoinerStartsWhileTheTokenSourceIsDown(t *testing.T) {
	t.Parallel()
	member := newEnrollingMember(t)
	source := newTokenSource(t, member.mint)
	hd := home.New(t.TempDir())
	nodeID := glid.New().String()
	cfg := joinerConfig(member.addr, source.url())
	if _, err := bootCluster(t, cfg, hd, nodeID, 30*time.Second); err != nil {
		t.Fatalf("first boot: %v", err)
	}

	// The token-serving node is down, as it is on a whole-cluster restart
	// until Raft has quorum. Nothing listens at the URL.
	source.srv.Close()
	ctls, err := bootCluster(t, cfg, hd, nodeID, 20*time.Second)
	if err != nil {
		t.Fatalf("restart with the token source down: %v", err)
	}
	requireCertFrom(t, ctls, member.ca, nodeID)
}

// homeWithoutACertificate prepares a home holding some of a member's state
// but no loadable certificate, and returns the node ID that home names.
type homeWithoutACertificate struct {
	name    string
	prepare func(t *testing.T, hd home.Dir, ca tlsutil.CAKeyPair) string
}

// identityOf resolves the node identity a home holds, minting it on first use.
func identityOf(t *testing.T, hd home.Dir) string {
	t.Helper()
	if err := hd.EnsureExists(); err != nil {
		t.Fatal(err)
	}
	id, err := resolveNodeID(hd, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

var homesWithoutACertificate = []homeWithoutACertificate{
	{
		name:    "empty home",
		prepare: func(*testing.T, home.Dir, tlsutil.CAKeyPair) string { return glid.New().String() },
	},
	{
		name: "node identity only",
		prepare: func(t *testing.T, hd home.Dir, _ tlsutil.CAKeyPair) string {
			return identityOf(t, hd)
		},
	},
	{
		name: "raft directory with partial state",
		prepare: func(t *testing.T, hd home.Dir, _ tlsutil.CAKeyPair) string {
			nodeID := identityOf(t, hd)
			snapDir := hd.RaftGroupDir("cluster-ctl")
			if err := os.MkdirAll(snapDir, 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(snapDir, "half-written"), []byte("partial"), 0o600); err != nil {
				t.Fatal(err)
			}
			return nodeID
		},
	},
	{
		name: "certificate write interrupted before its rename",
		prepare: func(t *testing.T, hd home.Dir, ca tlsutil.CAKeyPair) string {
			nodeID := identityOf(t, hd)
			cert, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, nodeID, cluster.LaneSANs)
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(map[string]string{
				"ca_cert_pem":      string(ca.CertPEM),
				"cluster_cert_pem": string(cert.CertPEM),
				"cluster_key_pem":  string(cert.KeyPEM),
			})
			if err != nil {
				t.Fatal(err)
			}
			dir, base := filepath.Split(hd.ClusterTLSPath())
			if err := os.WriteFile(filepath.Join(dir, "."+base+".123456"), data, 0o600); err != nil {
				t.Fatal(err)
			}
			return nodeID
		},
	},
}

// A node short of a loadable certificate is not a member, whatever else its
// home holds, and cannot start without the token.
func TestHomeWithoutACertificateStillNeedsTheToken(t *testing.T) {
	t.Parallel()
	for _, tc := range homesWithoutACertificate {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			member := newEnrollingMember(t)
			hd := home.New(t.TempDir())
			nodeID := tc.prepare(t, hd, member.ca)

			refusing := newTokenSource(t, member.mint)
			cfg := joinerConfig(member.addr, refusing.url())
			cfg.BootstrapTokenSecret = "not-the-serve-secret"
			if _, err := bootCluster(t, cfg, hd, nodeID, 20*time.Second); err == nil {
				t.Fatal("the node started without a token")
			} else if !strings.Contains(err.Error(), "resolve bootstrap token") {
				t.Fatalf("failed for another reason than the token: %v", err)
			}
			if got := refusing.hits.Load(); got != 1 {
				t.Fatalf("token source asked %d times, want 1", got)
			}

			source := newTokenSource(t, member.mint)
			ctls, err := bootCluster(t, joinerConfig(member.addr, source.url()), hd, nodeID, 30*time.Second)
			if err != nil {
				t.Fatalf("boot with the token: %v", err)
			}
			if got := source.hits.Load(); got != 1 {
				t.Fatalf("token source asked %d times, want 1", got)
			}
			requireCertFrom(t, ctls, member.ca, nodeID)
		})
	}
}

// An unreadable certificate file is neither a membership to resume nor a
// reason to enrol over it: the node refuses to start and leaves the file for
// the operator.
func TestUnreadableCertificateRefusesToStart(t *testing.T) {
	t.Parallel()
	member := newEnrollingMember(t)
	source := newTokenSource(t, member.mint)
	hd := home.New(t.TempDir())
	if err := hd.EnsureExists(); err != nil {
		t.Fatal(err)
	}
	truncated := []byte(`{"ca_cert_pem":"-----BEGIN CERT`)
	if err := os.WriteFile(hd.ClusterTLSPath(), truncated, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := bootCluster(t, joinerConfig(member.addr, source.url()), hd, glid.New().String(), 20*time.Second)
	if err == nil {
		t.Fatal("the node started on an unreadable certificate file")
	}
	got, readErr := os.ReadFile(hd.ClusterTLSPath())
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(got) != string(truncated) {
		t.Fatal("the unreadable certificate file was replaced by a fresh enrolment")
	}
	if n := source.hits.Load(); n != 0 {
		t.Fatalf("token source asked %d times by a node that cannot start, want 0", n)
	}
}

// A fresh joiner whose token source never answers fails the way it always
// has: it does not start, and says why.
func TestFreshJoinerFailsWhenTheTokenSourceNeverAnswers(t *testing.T) {
	t.Parallel()
	member := newEnrollingMember(t)
	source := newTokenSource(t, member.mint)
	source.srv.Close()

	_, err := bootCluster(t, joinerConfig(member.addr, source.url()), home.New(t.TempDir()), glid.New().String(), 3*time.Second)
	if err == nil {
		t.Fatal("a fresh joiner started without ever receiving a token")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "resolve bootstrap token") {
		t.Fatalf("err = %v, want the token resolution to run out of time", err)
	}
}

func TestTokenURLWithoutJoinAddressIsRefusedOnFirstBoot(t *testing.T) {
	t.Parallel()
	cfg := joinerConfig("", "http://127.0.0.1:1/cluster/bootstrap-token")
	_, err := bootCluster(t, cfg, home.New(t.TempDir()), glid.New().String(), 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "without --join-addr") {
		t.Fatalf("err = %v, want the missing join address named", err)
	}
}
