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

// selfSignedCert creates a self-signed certificate for fingerprint tests.
// isCA controls the basic-constraints CA bit, which verifyCAFingerprint
// uses to decide which of its two matching passes finds the cert.
func selfSignedCert(t *testing.T, commonName string, isCA bool) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now(),
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
	return cert
}

func fingerprintOf(cert *x509.Certificate) []byte {
	h := sha256.Sum256(cert.Raw)
	return h[:]
}

func TestVerifyCAFingerprint(t *testing.T) {
	t.Parallel()

	caCert := selfSignedCert(t, "Test CA", true)
	otherCert := selfSignedCert(t, "Other CA", true)
	leafCert := selfSignedCert(t, "leaf.example.com", false)

	cases := []struct {
		name         string
		peerCerts    []*x509.Certificate
		expectedHash []byte
		wantErr      bool
	}{
		{
			name:         "accepts a matching CA cert",
			peerCerts:    []*x509.Certificate{caCert},
			expectedHash: fingerprintOf(caCert),
			wantErr:      false,
		},
		{
			name:         "rejects a fingerprint mismatch",
			peerCerts:    []*x509.Certificate{caCert},
			expectedHash: fingerprintOf(otherCert),
			wantErr:      true,
		},
		{
			name:         "rejects an empty chain",
			peerCerts:    nil,
			expectedHash: fingerprintOf(caCert),
			wantErr:      true,
		},
		{
			name:         "falls back to fingerprinting a non-CA-only chain",
			peerCerts:    []*x509.Certificate{leafCert},
			expectedHash: fingerprintOf(leafCert),
			wantErr:      false,
		},
		{
			name:         "falls back correctly when a non-matching CA precedes the match",
			peerCerts:    []*x509.Certificate{otherCert, leafCert},
			expectedHash: fingerprintOf(leafCert),
			wantErr:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := verifyCAFingerprint(tc.peerCerts, tc.expectedHash)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
		})
	}
}
