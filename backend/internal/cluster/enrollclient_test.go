package cluster

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"
)

// selfSignedCert creates a self-signed certificate. isCA sets the
// basic-constraints CA bit so the certificate can issue others.
func selfSignedCert(t *testing.T, commonName string, isCA bool) *x509.Certificate {
	t.Helper()
	cert, _ := selfSignedPair(t, commonName, isCA)
	return cert
}

func selfSignedPair(t *testing.T, commonName string, isCA bool) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert, key
}

// issuedBy creates a server certificate signed by the given CA, which is the
// shape a real cluster presents: leaf first, issuer behind it.
func issuedBy(t *testing.T, commonName string, ca *x509.Certificate, caKey *ecdsa.PrivateKey) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert
}

func fingerprintOf(cert *x509.Certificate) []byte {
	h := sha256.Sum256(cert.Raw)
	return h[:]
}

// The join token pins the cluster CA by fingerprint, and that certificate is
// public — anyone who has watched a handshake holds a copy. Finding it
// somewhere in the presented chain therefore proves nothing; the certificate
// terminating the connection has to chain to it. Otherwise a joiner hands its
// token to whoever answered.
func TestVerifyEnrollmentChain(t *testing.T) {
	t.Parallel()

	ca, caKey := selfSignedPair(t, "Test CA", true)
	genuine := issuedBy(t, "gastrolog-cluster", ca, caKey)
	otherCA := selfSignedCert(t, "Other CA", true)
	impostor := selfSignedCert(t, "attacker.example.com", false)

	cases := []struct {
		name         string
		peerCerts    []*x509.Certificate
		expectedHash []byte
		wantErr      bool
		why          string
	}{
		{
			name:         "the shape a real cluster presents",
			peerCerts:    []*x509.Certificate{genuine, ca},
			expectedHash: fingerprintOf(ca),
			wantErr:      false,
			why:          "the leaf is issued by the pinned CA",
		},
		{
			name:         "an impostor holding a copy of the public CA",
			peerCerts:    []*x509.Certificate{impostor, ca},
			expectedHash: fingerprintOf(ca),
			wantErr:      true,
			why:          "the pinned CA is present but did not issue the leaf",
		},
		{
			name:         "a token pinning the leaf itself",
			peerCerts:    []*x509.Certificate{genuine},
			expectedHash: fingerprintOf(genuine),
			wantErr:      false,
			why:          "pinning an exact certificate is stronger than pinning its issuer",
		},
		{
			name:         "a self-signed single certificate",
			peerCerts:    []*x509.Certificate{ca},
			expectedHash: fingerprintOf(ca),
			wantErr:      false,
			why:          "minimal deployments present one certificate and pin it",
		},
		{
			name:         "a chain carrying no pinned certificate",
			peerCerts:    []*x509.Certificate{genuine, otherCA},
			expectedHash: fingerprintOf(ca),
			wantErr:      true,
			why:          "nothing presented matches the token",
		},
		{
			name:         "an empty chain",
			peerCerts:    nil,
			expectedHash: fingerprintOf(ca),
			wantErr:      true,
			why:          "there is nothing to verify",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := verifyEnrollmentChain(tc.peerCerts, tc.expectedHash)
			if tc.wantErr && err == nil {
				t.Fatalf("accepted the connection, but %s", tc.why)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("rejected the connection (%v), but %s", err, tc.why)
			}
		})
	}
}
