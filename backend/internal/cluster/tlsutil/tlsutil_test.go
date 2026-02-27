package tlsutil_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"gastrolog/internal/cluster/tlsutil"
)

func TestGenerateCA(t *testing.T) {
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	// Parse the CA cert.
	block, _ := pem.Decode(ca.CertPEM)
	if block == nil {
		t.Fatal("failed to decode CA cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	if !cert.IsCA {
		t.Error("expected CA certificate")
	}
	if cert.Subject.CommonName != "gastrolog-cluster-ca" {
		t.Errorf("unexpected CN: %s", cert.Subject.CommonName)
	}
	if cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Error("CA cert missing CertSign key usage")
	}
}

func TestGenerateClusterCert(t *testing.T) {
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	cluster, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, []string{"node1.local", "10.0.0.1"})
	if err != nil {
		t.Fatalf("GenerateClusterCert: %v", err)
	}

	// Parse and verify the cluster cert.
	block, _ := pem.Decode(cluster.CertPEM)
	if block == nil {
		t.Fatal("failed to decode cluster cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse cluster cert: %v", err)
	}

	// Verify signed by CA.
	caBlock, _ := pem.Decode(ca.CertPEM)
	caCert, _ := x509.ParseCertificate(caBlock.Bytes)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	chains, err := cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		t.Fatalf("verify cluster cert: %v", err)
	}
	if len(chains) == 0 {
		t.Error("no valid chains found")
	}

	// Check ExtKeyUsage.
	hasServer, hasClient := false, false
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth {
			hasServer = true
		}
		if usage == x509.ExtKeyUsageClientAuth {
			hasClient = true
		}
	}
	if !hasServer {
		t.Error("missing ServerAuth ExtKeyUsage")
	}
	if !hasClient {
		t.Error("missing ClientAuth ExtKeyUsage")
	}

	// Check SANs.
	foundLocalhost := false
	for _, dns := range cert.DNSNames {
		if dns == "localhost" {
			foundLocalhost = true
		}
	}
	if !foundLocalhost {
		t.Error("missing localhost SAN")
	}

	foundExtraDNS := false
	for _, dns := range cert.DNSNames {
		if dns == "node1.local" {
			foundExtraDNS = true
		}
	}
	if !foundExtraDNS {
		t.Error("missing extra DNS SAN node1.local")
	}

	foundExtraIP := false
	for _, ip := range cert.IPAddresses {
		if ip.String() == "10.0.0.1" {
			foundExtraIP = true
		}
	}
	if !foundExtraIP {
		t.Error("missing extra IP SAN 10.0.0.1")
	}

	// Verify the cert+key form a valid TLS pair.
	if _, err := tls.X509KeyPair(cluster.CertPEM, cluster.KeyPEM); err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
}

func TestJoinTokenRoundTrip(t *testing.T) {
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	token, err := tlsutil.GenerateJoinToken(ca.CertPEM)
	if err != nil {
		t.Fatalf("GenerateJoinToken: %v", err)
	}

	secret, caHash, err := tlsutil.ParseJoinToken(token)
	if err != nil {
		t.Fatalf("ParseJoinToken: %v", err)
	}
	if secret == "" {
		t.Error("empty secret")
	}
	if caHash == "" {
		t.Error("empty CA hash")
	}

	// Verify the CA fingerprint matches.
	ok, err := tlsutil.VerifyCAFingerprint(ca.CertPEM, caHash)
	if err != nil {
		t.Fatalf("VerifyCAFingerprint: %v", err)
	}
	if !ok {
		t.Error("CA fingerprint mismatch")
	}

	// Verify a different CA doesn't match.
	ca2, _ := tlsutil.GenerateCA()
	ok, err = tlsutil.VerifyCAFingerprint(ca2.CertPEM, caHash)
	if err != nil {
		t.Fatalf("VerifyCAFingerprint (different CA): %v", err)
	}
	if ok {
		t.Error("expected CA fingerprint mismatch for different CA")
	}
}

func TestParseJoinTokenInvalid(t *testing.T) {
	tests := []struct {
		name  string
		token string
	}{
		{"no colon", "abcdef1234"},
		{"bad secret", "zzzz:abcd"},
		{"bad hash", "abcd:zzzz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := tlsutil.ParseJoinToken(tt.token)
			if err == nil {
				t.Error("expected error for invalid token")
			}
		})
	}
}
