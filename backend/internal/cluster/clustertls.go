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

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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
	data, err := os.ReadFile(path) //nolint:gosec // G304: path from trusted home dir
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
// enabling hot-reload. ClientAuth is VerifyClientCertIfGiven to allow
// the Enroll RPC from nodes without client certs.
func (c *ClusterTLS) ServerTLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			st := c.state.Load()
			if st == nil {
				return nil, errors.New("cluster TLS not loaded")
			}
			return &st.Cert, nil
		},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			st := c.state.Load()
			if st == nil {
				return nil, errors.New("cluster TLS not loaded")
			}
			return &tls.Config{
				Certificates: []tls.Certificate{st.Cert},
				ClientCAs:    st.CAPool,
				ClientAuth:   tls.VerifyClientCertIfGiven,
				MinVersion:   tls.VersionTLS13,
			}, nil
		},
		ClientAuth: tls.VerifyClientCertIfGiven,
		MinVersion: tls.VersionTLS13,
	}
}

// ClientTLSConfig returns a tls.Config for dialing other cluster nodes.
func (c *ClusterTLS) ClientTLSConfig() *tls.Config {
	st := c.state.Load()
	if st == nil {
		return nil
	}
	return &tls.Config{
		Certificates: []tls.Certificate{st.Cert},
		RootCAs:      st.CAPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	}
}

// TransportCredentials returns gRPC transport credentials for dialing
// other cluster nodes with mTLS. If TLS state has not been loaded yet,
// the returned credentials fall back to insecure and automatically
// upgrade once Load is called. This allows the Raft transport to be
// created before TLS material is available (bootstrap flow).
func (c *ClusterTLS) TransportCredentials() credentials.TransportCredentials {
	return &dynamicCreds{ctls: c}
}

// dynamicCreds implements credentials.TransportCredentials by reading from
// the ClusterTLS atomic pointer on each handshake. This enables the Raft
// gRPC transport to start before TLS is available and seamlessly upgrade
// when TLS material is loaded.
type dynamicCreds struct {
	ctls *ClusterTLS
}

func (d *dynamicCreds) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return d.current().ClientHandshake(ctx, authority, rawConn)
}

func (d *dynamicCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return d.current().ServerHandshake(rawConn)
}

func (d *dynamicCreds) Info() credentials.ProtocolInfo {
	return d.current().Info()
}

func (d *dynamicCreds) Clone() credentials.TransportCredentials {
	return &dynamicCreds{ctls: d.ctls}
}

func (d *dynamicCreds) OverrideServerName(name string) error {
	return nil
}

func (d *dynamicCreds) current() credentials.TransportCredentials {
	st := d.ctls.state.Load()
	if st == nil {
		return insecure.NewCredentials()
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{st.Cert},
		RootCAs:      st.CAPool,
		// All cluster nodes share one cert with "localhost" in SANs.
		// Override ServerName so TLS verification succeeds regardless of
		// how the peer address resolves (e.g., [::]:4585 → "::" as host).
		ServerName: "localhost",
		MinVersion: tls.VersionTLS13,
	})
}
