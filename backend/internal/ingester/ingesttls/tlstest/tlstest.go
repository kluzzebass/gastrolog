// Package tlstest mints throwaway certificate fixtures for ingester TLS
// tests: a CA, leaves it signs, and a cert manager holding them by name.
// Test support only — nothing here belongs in a serving path.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"gastrolog/internal/cert"
)

// Pair is a PEM-encoded certificate and key.
type Pair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// CA is a throwaway certificate authority that signs leaves.
type CA struct {
	Pair Pair
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// NewCA mints a CA.
func NewCA(t *testing.T, cn string) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return &CA{
		Pair: Pair{
			CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		},
		cert: parsed,
		key:  key,
	}
}

// Leaf mints a certificate signed by the CA with the given CN and SANs
// (DNS names; IP literals are detected and added as IP SANs).
func (ca *CA) Leaf(t *testing.T, cn string, sans []string) Pair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, s := range sans {
		if ip := net.ParseIP(s); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, s)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return Pair{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// Pool builds an x509 pool trusting the CA.
func (ca *CA) Pool(t *testing.T) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.Pair.CertPEM) {
		t.Fatal("CA PEM contains no certificates")
	}
	return pool
}

// Manager loads named pairs into a cert manager.
func Manager(t *testing.T, pairs map[string]Pair) *cert.Manager {
	t.Helper()
	m := cert.New(cert.Config{})
	for name, p := range pairs {
		if err := m.AddFromPEM(name, string(p.CertPEM), string(p.KeyPEM)); err != nil {
			t.Fatalf("AddFromPEM(%s): %v", name, err)
		}
	}
	return m
}
