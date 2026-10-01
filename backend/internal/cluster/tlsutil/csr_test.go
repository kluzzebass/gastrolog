package tlsutil_test

// A certificate signing request is a proposal, not an instruction. Everything
// the issued certificate asserts is decided by the issuer; the request
// contributes a public key and a proof that its sender holds the private one.

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"gastrolog/internal/cluster/tlsutil"
)

func testCA(t *testing.T) tlsutil.CAKeyPair {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func parseCert(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("decode certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// The whole point of issuing per-node certificates: a joiner that asks to be
// called node-1 gets called what the cluster decided, or the identity in the
// certificate would be self-asserted and worth nothing.
func TestSignCSRIgnoresTheNameTheRequesterAsksFor(t *testing.T) {
	t.Parallel()
	ca := testCA(t)

	csrPEM, _, err := tlsutil.GenerateCSR("node-1")
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := tlsutil.SignCSR(ca.CertPEM, ca.KeyPEM, csrPEM, "node-2", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := tlsutil.NodeIDFromCert(parseCert(t, certPEM)); got != "node-2" {
		t.Fatalf("certificate names %q; the requester's own claim was honoured", got)
	}
}

// The issued certificate must describe the key in the request. Binding it to
// any other key would hand the holder of that other key an identity.
func TestSignCSRCertifiesTheKeyInTheRequest(t *testing.T) {
	t.Parallel()
	ca := testCA(t)

	csrPEM, keyPEM, err := tlsutil.GenerateCSR("node-2")
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := tlsutil.SignCSR(ca.CertPEM, ca.KeyPEM, csrPEM, "node-2", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Pairing succeeds only if the certificate's public key matches this key.
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		t.Fatalf("the issued certificate does not describe the requester's key: %v", err)
	}
}

func TestSignCSRRefusesAnUnsignedRequest(t *testing.T) {
	t.Parallel()
	ca := testCA(t)

	csrPEM, _, err := tlsutil.GenerateCSR("node-2")
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(csrPEM)
	der := make([]byte, len(block.Bytes))
	copy(der, block.Bytes)
	der[len(der)/3] ^= 0xff
	tampered := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})

	if _, err := tlsutil.SignCSR(ca.CertPEM, ca.KeyPEM, tampered, "node-2", nil); err == nil {
		t.Fatal("a request whose signature does not cover its contents was signed")
	}
}

// An unnamed certificate cannot be checked against the cluster's membership,
// so it must not be issuable in the first place.
func TestNodeCertsMustBeNamed(t *testing.T) {
	t.Parallel()
	ca := testCA(t)

	if _, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, "", nil); err == nil {
		t.Fatal("issued a certificate naming nobody")
	}
	if _, _, err := tlsutil.GenerateCSR(""); err == nil {
		t.Fatal("built a request naming nobody")
	}
}
