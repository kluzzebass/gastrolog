package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"

	"gastrolog/internal/multiraft"

	"google.golang.org/grpc/credentials"
)

// tlsFile is the on-disk format for persisted cluster TLS material.
type tlsFile struct {
	CACertPEM      string `json:"ca_cert_pem"`
	ClusterCertPEM string `json:"cluster_cert_pem"`
	ClusterKeyPEM  string `json:"cluster_key_pem"`
}

// TLSState holds parsed TLS material for the cluster port.
type TLSState struct {
	Cert   tls.Certificate
	CACert *x509.Certificate
	CAPool *x509.CertPool
}

// ClusterTLS provides atomic access to TLS configuration for the cluster port.
// The gRPC server's GetCertificate/GetConfigForClient callbacks read from an
// atomic pointer, enabling cert rotation without server restart.
type ClusterTLS struct {
	state atomic.Pointer[TLSState]
}

// NewClusterTLS creates a new ClusterTLS holder. Load must be called to
// populate it with TLS material before it can be used.
func NewClusterTLS() *ClusterTLS {
	return &ClusterTLS{}
}

// Load parses PEM-encoded certificate material and atomically swaps the
// TLS state. New connections will use the updated certificates; existing
// connections drain naturally.
func (c *ClusterTLS) Load(certPEM, keyPEM, caCertPEM []byte) error {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}

	block, _ := pem.Decode(caCertPEM)
	if block == nil {
		return errors.New("decode CA cert PEM: no PEM block found")
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}

	// Append CA cert to the chain so the server presents the full chain
	// during TLS handshake. This allows TOFU clients (enrollment) to
	// verify the CA fingerprint from the join token.
	cert.Certificate = append(cert.Certificate, block.Bytes)

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	c.state.Store(&TLSState{
		Cert:   cert,
		CACert: caCert,
		CAPool: pool,
	})
	return nil
}

// State returns the current TLS state, or nil if Load has not been called.
func (c *ClusterTLS) State() *TLSState {
	return c.state.Load()
}

// SaveFile persists the raw PEM material to a local JSON file so it's
// available on restart without depending on Raft snapshots or quorum.
// The file is written atomically (write-tmp + rename) with 0600 permissions.
func SaveFile(path string, certPEM, keyPEM, caCertPEM []byte) error {
	data, err := json.Marshal(tlsFile{
		CACertPEM:      string(caCertPEM),
		ClusterCertPEM: string(certPEM),
		ClusterKeyPEM:  string(keyPEM),
	})
	if err != nil {
		return fmt.Errorf("marshal cluster TLS: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write cluster TLS temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename cluster TLS file: %w", err)
	}
	return nil
}

// LoadFile reads persisted TLS material from a local JSON file and calls
// Load to populate the atomic state. Returns false if the file doesn't exist.
func (c *ClusterTLS) LoadFile(path string) (bool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path from trusted home dir //ok:os-readfile a few KB of PEM in one JSON object, read once at startup
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read cluster TLS file: %w", err)
	}

	var f tlsFile
	if err := json.Unmarshal(data, &f); err != nil {
		return false, fmt.Errorf("unmarshal cluster TLS file: %w", err)
	}

	if err := c.Load([]byte(f.ClusterCertPEM), []byte(f.ClusterKeyPEM), []byte(f.CACertPEM)); err != nil {
		return false, fmt.Errorf("load cluster TLS from file: %w", err)
	}
	return true, nil
}

// ServerTLSConfig returns a tls.Config for the cluster gRPC server.
// GetCertificate and GetConfigForClient read from the atomic pointer,
// enabling hot-reload. GetConfigForClient also picks the client-certificate
// policy per connection from the ClientHello SNI — see clientAuthForSNI.
func (c *ClusterTLS) ServerTLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			st := c.state.Load()
			if st == nil {
				return nil, errors.New("cluster TLS not loaded")
			}
			return &st.Cert, nil
		},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			st := c.state.Load()
			if st == nil {
				return nil, errors.New("cluster TLS not loaded")
			}
			return &tls.Config{
				Certificates: []tls.Certificate{st.Cert},
				ClientCAs:    st.CAPool,
				ClientAuth:   clientAuthForSNI(hello.ServerName),
				MinVersion:   tls.VersionTLS13,
			}, nil
		},
		ClientAuth: tls.VerifyClientCertIfGiven,
		MinVersion: tls.VersionTLS13,
	}
}

// clientAuthForSNI selects the client-certificate policy for one inbound
// connection. Raft lane SNIs demand a cluster certificate at the handshake:
// hashicorp/raft trusts its transport unconditionally, so an anonymous lane
// connection would drive consensus. Every other SNI reaches the service lane,
// which stays certificate-optional because Enroll is how a joining node gets
// the certificate it does not yet have.
func clientAuthForSNI(sni string) tls.ClientAuthType {
	if multiraft.IsRaftLaneSNI(sni) {
		return tls.RequireAndVerifyClientCert
	}
	return tls.VerifyClientCertIfGiven
}

// ClientTLSConfig returns a tls.Config for dialing other cluster nodes on the
// service lane (ClusterService, chunk transfer, search forward, etc.).
func (c *ClusterTLS) ClientTLSConfig() *tls.Config {
	return c.clientTLSConfigForServerName(SNIServiceLane)
}

// ClientTLSConfigForRaft returns a tls.Config for dialing the raft lane.
func (c *ClusterTLS) ClientTLSConfigForRaft() *tls.Config {
	return c.clientTLSConfigForServerName(SNIRaftLane)
}

func (c *ClusterTLS) clientTLSConfigForServerName(serverName string) *tls.Config {
	st := c.state.Load()
	if st == nil {
		return nil
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{st.Cert},
		RootCAs:      st.CAPool,
		MinVersion:   tls.VersionTLS13,
	}
	if multiraft.IsRaftLaneSNI(serverName) {
		// Per-group raft SNIs (gastrolog-raft.config, gastrolog-raft.vault…)
		// must stay in the ClientHello for inbound SNI demux. Node certs only
		// SAN gastrolog-raft / gastrolog-cluster, so skip default hostname
		// verification and verify the cluster CA chain in VerifyConnection.
		cfg.ServerName = serverName
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("cluster TLS: no peer certificate")
			}
			opts := x509.VerifyOptions{Roots: st.CAPool}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
		return cfg
	}
	cfg.ServerName = serverName
	return cfg
}

// TransportCredentials returns gRPC transport credentials for the service lane.
func (c *ClusterTLS) TransportCredentials() credentials.TransportCredentials {
	return c.TransportCredentialsForServerName(SNIServiceLane)
}

// TransportCredentialsForRaft returns gRPC transport credentials for the raft lane.
func (c *ClusterTLS) TransportCredentialsForRaft() credentials.TransportCredentials {
	return c.TransportCredentialsForServerName(SNIRaftLane)
}

// TransportCredentialsForServerName returns lane-specific outbound credentials.
func (c *ClusterTLS) TransportCredentialsForServerName(serverName string) credentials.TransportCredentials {
	return &laneDynamicCreds{ctls: c, serverName: serverName}
}

// ErrClusterTLSUnloaded is returned by a cluster-credential handshake
// attempted before the node holds cluster TLS material. Dialling on is not
// an option: the peer's raft lanes demand a client certificate, and a
// plaintext connection to a TLS listener cannot complete either way.
var ErrClusterTLSUnloaded = errors.New("cluster TLS not loaded")

// laneDynamicCreds implements credentials.TransportCredentials with a fixed
// TLS ServerName for lane-specific verification after SNI demux.
type laneDynamicCreds struct {
	ctls       *ClusterTLS
	serverName string
}

func (d *laneDynamicCreds) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	creds, err := d.current()
	if err != nil {
		return nil, nil, err
	}
	return creds.ClientHandshake(ctx, authority, rawConn)
}

func (d *laneDynamicCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	creds, err := d.current()
	if err != nil {
		return nil, nil, err
	}
	return creds.ServerHandshake(rawConn)
}

func (d *laneDynamicCreds) Info() credentials.ProtocolInfo {
	if creds, err := d.current(); err == nil {
		return creds.Info()
	}
	// These credentials are TLS or nothing. Reporting the protocol they
	// will use keeps an inspecting caller from reading the unloaded window
	// as a plaintext connection.
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (d *laneDynamicCreds) Clone() credentials.TransportCredentials {
	return &laneDynamicCreds{ctls: d.ctls, serverName: d.serverName}
}

func (d *laneDynamicCreds) OverrideServerName(name string) error {
	return nil
}

// current resolves the TLS material for this lane at handshake time.
//
// Unloaded state fails the handshake. A caller that genuinely runs without
// cluster TLS asks for insecure credentials by name; these credentials
// exist to carry cluster identity, and quietly handing back plaintext when
// the identity is missing is a downgrade the operator never sees.
func (d *laneDynamicCreds) current() (credentials.TransportCredentials, error) {
	cfg := d.ctls.clientTLSConfigForServerName(d.serverName)
	if cfg == nil {
		return nil, fmt.Errorf("%w (lane %s)", ErrClusterTLSUnloaded, d.serverName)
	}
	return credentials.NewTLS(cfg), nil
}
