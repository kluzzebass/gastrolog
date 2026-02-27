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
// The client uses InsecureSkipVerify with a custom VerifyPeerCertificate
// callback that checks the CA fingerprint from the token (TOFU model).
func Enroll(ctx context.Context, leaderAddr, tokenSecret, caHash, nodeID, nodeAddr string) (*EnrollResult, error) {
	expectedHash, err := hex.DecodeString(caHash)
	if err != nil {
		return nil, fmt.Errorf("decode CA hash from token: %w", err)
	}

	// TOFU TLS config: skip normal verification, verify CA fingerprint manually.
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: intentional TOFU — we verify CA fingerprint below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("server presented no certificates")
			}
			// Walk the chain to find the CA (self-signed) cert and check its fingerprint.
			for _, raw := range rawCerts {
				cert, err := x509.ParseCertificate(raw)
				if err != nil {
					continue
				}
				if cert.IsCA {
					hash := sha256.Sum256(raw)
					if hex.EncodeToString(hash[:]) == hex.EncodeToString(expectedHash) {
						return nil
					}
				}
			}
			// If no CA cert found in chain, check the leaf's issuer by fingerprinting
			// the raw DER of each cert in the chain.
			for _, raw := range rawCerts {
				hash := sha256.Sum256(raw)
				if hex.EncodeToString(hash[:]) == hex.EncodeToString(expectedHash) {
					return nil
				}
			}
			return errors.New("CA fingerprint mismatch: server CA does not match join token")
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
		NodeId:      nodeID,
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
