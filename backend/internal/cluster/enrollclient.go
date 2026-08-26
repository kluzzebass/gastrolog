package cluster

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// EnrollResult holds the TLS material returned by a successful enrollment.
type EnrollResult struct {
	CACertPEM      []byte
	ClusterCertPEM []byte
	ClusterKeyPEM  []byte
}

// Enroll connects to the leader's cluster port and enrolls this node.
// The joinToken format is "<hex-secret>:<hex-sha256(CA DER)>".
//
// The client uses InsecureSkipVerify with a custom VerifyConnection
// callback that checks the CA fingerprint from the token (TOFU model).
func Enroll(ctx context.Context, leaderAddr, tokenSecret, caHash, nodeID, nodeAddr string) (*EnrollResult, error) {
	expectedHash, err := hex.DecodeString(caHash)
	if err != nil {
		return nil, fmt.Errorf("decode CA hash from token: %w", err)
	}

	// TOFU TLS config: skip normal verification, verify CA fingerprint manually.
	// VerifyConnection runs on every handshake, including a resumed one, so
	// the fingerprint check can't be bypassed by session resumption the way
	// a VerifyPeerCertificate-only check could.
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: intentional TOFU — we verify CA fingerprint below
		VerifyConnection: func(cs tls.ConnectionState) error {
			return verifyCAFingerprint(cs.PeerCertificates, expectedHash)
		},
		MinVersion: tls.VersionTLS13,
	}

	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial leader %s: %w", leaderAddr, err)
	}
	defer func() { _ = conn.Close() }()

	req := &gastrologv1.EnrollRequest{
		TokenSecret: tokenSecret,
		NodeId:      []byte(nodeID),
		NodeAddr:    nodeAddr,
	}
	resp := &gastrologv1.EnrollResponse{}

	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Enroll", req, resp); err != nil {
		return nil, fmt.Errorf("enroll RPC: %w", err)
	}

	return &EnrollResult{
		CACertPEM:      resp.GetCaCertPem(),
		ClusterCertPEM: resp.GetClusterCertPem(),
		ClusterKeyPEM:  resp.GetClusterKeyPem(),
	}, nil
}

// verifyCAFingerprint checks that the peer's chain contains a certificate
// whose SHA-256 fingerprint matches expectedHash (the CA hash from the join
// token), per the TOFU model. It falls back to fingerprinting every cert in
// the chain when none is marked as a CA, since self-signed leaf certs and
// certs from minimal chains don't always set the CA basic constraint.
func verifyCAFingerprint(peerCerts []*x509.Certificate, expectedHash []byte) error {
	if len(peerCerts) == 0 {
		return errors.New("server presented no certificates")
	}
	for _, cert := range peerCerts {
		if cert.IsCA {
			hash := sha256.Sum256(cert.Raw)
			if hex.EncodeToString(hash[:]) == hex.EncodeToString(expectedHash) {
				return nil
			}
		}
	}
	for _, cert := range peerCerts {
		hash := sha256.Sum256(cert.Raw)
		if hex.EncodeToString(hash[:]) == hex.EncodeToString(expectedHash) {
			return nil
		}
	}
	return errors.New("CA fingerprint mismatch: server CA does not match join token")
}
