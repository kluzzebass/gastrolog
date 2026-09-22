package cluster

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster/tlsutil"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// EnrollResult holds the TLS material a node ends enrollment with: the
// cluster's trust root, the certificate the cluster issued for this node, and
// the private key that node generated for itself and never sent anywhere.
type EnrollResult struct {
	CACertPEM   []byte
	NodeCertPEM []byte
	NodeKeyPEM  []byte
}

// Enroll connects to a cluster member's cluster port and enrolls this node.
// The joinToken format is "<hex-secret>:<hex-sha256(CA DER)>".
//
// The client uses InsecureSkipVerify with a custom VerifyConnection
// callback that checks the CA fingerprint from the token (TOFU model).
//
// A key pair is generated here and only the certificate request is sent, so
// what comes back is a certificate for a key the cluster has never seen. The
// node ID in the request is a request: the member decides what the
// certificate says.
func Enroll(ctx context.Context, memberAddr, tokenSecret, caHash, nodeID, nodeAddr string) (*EnrollResult, error) {
	expectedHash, err := hex.DecodeString(caHash)
	if err != nil {
		return nil, fmt.Errorf("decode CA hash from token: %w", err)
	}

	csrPEM, keyPEM, err := tlsutil.GenerateCSR(nodeID)
	if err != nil {
		return nil, fmt.Errorf("generate certificate request: %w", err)
	}

	// TOFU TLS config: skip normal verification, verify CA fingerprint manually.
	// VerifyConnection runs on every handshake, including a resumed one, so
	// the fingerprint check can't be bypassed by session resumption the way
	// a VerifyPeerCertificate-only check could.
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: intentional TOFU — we verify CA fingerprint below
		VerifyConnection: func(cs tls.ConnectionState) error {
			return verifyEnrollmentChain(cs.PeerCertificates, expectedHash)
		},
		MinVersion: tls.VersionTLS13,
	}

	conn, err := grpc.NewClient(memberAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial cluster member %s: %w", memberAddr, err)
	}
	defer func() { _ = conn.Close() }()

	req := &gastrologv1.EnrollRequest{
		TokenSecret: tokenSecret,
		NodeId:      []byte(nodeID),
		NodeAddr:    nodeAddr,
		CsrPem:      csrPEM,
	}
	resp := &gastrologv1.EnrollResponse{}

	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Enroll", req, resp); err != nil {
		return nil, fmt.Errorf("enroll RPC: %w", err)
	}

	return &EnrollResult{
		CACertPEM:   resp.GetCaCertPem(),
		NodeCertPEM: resp.GetNodeCertPem(),
		NodeKeyPEM:  keyPEM,
	}, nil
}

// verifyEnrollmentChain checks that the server terminating this connection
// actually holds a key the join token vouches for.
//
// The token carries a SHA-256 of the cluster CA certificate, which is public:
// anyone who has seen a handshake has a copy. So finding that fingerprint
// somewhere in the presented chain proves nothing on its own — an attacker
// can present their own leaf alongside the genuine CA and pass. The leaf has
// to chain to the pinned CA, or the joiner hands its token to whoever
// answered.
//
// A token that pins the leaf itself is also accepted: pinning an exact
// certificate is a stronger statement than chaining to an issuer, and it
// keeps single-certificate deployments working.
func verifyEnrollmentChain(peerCerts []*x509.Certificate, expectedHash []byte) error {
	if len(peerCerts) == 0 {
		return errors.New("server presented no certificates")
	}
	leaf := peerCerts[0]
	if certMatches(leaf, expectedHash) {
		return nil
	}

	var pinned *x509.Certificate
	for _, cert := range peerCerts[1:] {
		if certMatches(cert, expectedHash) {
			pinned = cert
			break
		}
	}
	if pinned == nil {
		return errors.New("CA fingerprint mismatch: server CA does not match join token")
	}

	roots := x509.NewCertPool()
	roots.AddCert(pinned)
	intermediates := x509.NewCertPool()
	for _, cert := range peerCerts[1:] {
		if cert != pinned {
			intermediates.AddCert(cert)
		}
	}
	// No DNS name to check: cluster certificates carry lane SANs, never the
	// address an operator typed. The pinned issuer is the whole assertion.
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("server certificate does not chain to the CA in the join token: %w", err)
	}
	return nil
}

// certMatches reports whether cert is the one the token pins. Constant-time
// so a mismatch reveals nothing about where it diverged.
func certMatches(cert *x509.Certificate, expectedHash []byte) bool {
	sum := sha256.Sum256(cert.Raw)
	return subtle.ConstantTimeCompare(sum[:], expectedHash) == 1
}
