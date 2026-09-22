package app

// Enrolment is the one call that issues a cluster identity in exchange for a
// secret, so what it does with a wrong secret, a borrowed node ID or an
// unproven key matters as much as what it does with a legitimate joiner.

import (
	"context"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"log/slog"
	"strings"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// enrollFixture builds a store holding a real cluster CA and returns the
// handler plus the join secret a legitimate joiner would present.
func enrollFixture(t *testing.T) (handler func(context.Context, *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error), secret string) {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	key, err := tlsutil.GenerateJoinTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	token, err := tlsutil.MintJoinToken(key, ca.CertPEM, tlsutil.DefaultJoinTokenTTL)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err = tlsutil.ParseJoinToken(token)
	if err != nil {
		t.Fatal(err)
	}

	store := sysmem.NewStore()
	if err := store.PutClusterTLS(context.Background(), system.ClusterTLS{
		CACertPEM:    string(ca.CertPEM),
		CAKeyPEM:     string(ca.KeyPEM),
		JoinTokenKey: hex.EncodeToString(key),
	}); err != nil {
		t.Fatal(err)
	}
	// A nil cluster server stands for a node with no readable configuration;
	// the ID-already-taken refusal is exercised against inConfiguration.
	return makeEnrollHandler(store, nil, slog.New(slog.DiscardHandler)), secret
}

// csrFor produces what a joiner sends: a request for nodeID, signed by a key
// that stays with the joiner.
func csrFor(t *testing.T, nodeID string) []byte {
	t.Helper()
	csrPEM, _, err := tlsutil.GenerateCSR(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return csrPEM
}

func TestEnrollIssuesACertificateNamingTheJoiner(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	resp, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: secret,
		NodeId:      []byte("node-2"),
		NodeAddr:    "node-2:4566",
		CsrPem:      csrFor(t, "node-2"),
	})
	if err != nil {
		t.Fatalf("a joiner with the real token was refused: %v", err)
	}
	if len(resp.GetCaCertPem()) == 0 {
		t.Fatal("enrolment returned no trust root")
	}

	block, _ := pem.Decode(resp.GetNodeCertPem())
	if block == nil {
		t.Fatal("enrolment returned no certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse issued certificate: %v", err)
	}
	if got := tlsutil.NodeIDFromCert(cert); got != "node-2" {
		t.Fatalf("certificate names %q, want node-2 — a misnamed certificate cannot be checked against membership", got)
	}
}

// The response must not carry a private key. One that did would be a key the
// cluster had seen, which is the thing per-node certificates exist to stop.
func TestEnrollResponseCarriesNoPrivateKey(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	resp, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: secret,
		NodeId:      []byte("node-2"),
		CsrPem:      csrFor(t, "node-2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range [][]byte{resp.GetCaCertPem(), resp.GetNodeCertPem()} {
		if strings.Contains(string(field), "PRIVATE KEY") {
			t.Fatal("enrolment sent a private key over the wire")
		}
	}
}

func TestEnrollRejectsAWrongSecret(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	// Same length, differing in the last byte: a byte-by-byte comparison
	// answers this later than one differing in the first, which is the
	// signal a caller would walk the secret with.
	wrong := secret[:len(secret)-1] + "0"
	if wrong == secret {
		wrong = secret[:len(secret)-1] + "1"
	}

	if _, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: wrong,
		NodeId:      []byte("attacker"),
		CsrPem:      csrFor(t, "attacker"),
	}); err == nil {
		t.Fatal("a wrong token was accepted")
	}
}

// A certificate request is a public key plus a claim to hold the matching
// private one. Skipping the proof would let a caller be issued a certificate
// for a key it does not hold, replayed from a request it watched.
func TestEnrollRejectsAnUnprovenKey(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	if _, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: secret,
		NodeId:      []byte("node-2"),
		CsrPem:      tamperCSR(t, csrFor(t, "node-2")),
	}); err == nil {
		t.Fatal("a certificate request whose signature does not cover its contents was accepted")
	}
}

// tamperCSR flips a byte inside the request body, leaving the signature
// covering something other than what is now there.
func tamperCSR(t *testing.T, csrPEM []byte) []byte {
	t.Helper()
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		t.Fatal("decode CSR")
	}
	der := make([]byte, len(block.Bytes))
	copy(der, block.Bytes)
	der[len(der)/3] ^= 0xff
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}

func TestEnrollRejectsAMissingCertificateRequest(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	if _, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: secret,
		NodeId:      []byte("node-2"),
	}); err == nil {
		t.Fatal("enrolment issued a certificate with no key to issue it for")
	}
}

func TestEnrollRejectsAnUnnamedJoiner(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	if _, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: secret,
		CsrPem:      csrFor(t, "node-2"),
	}); err == nil {
		t.Fatal("enrolment issued a certificate naming nobody")
	}
}

// Guessing and log-flooding both need volume. Enrolment is retried with
// backoff and happens a handful of times in a cluster's life, so a ceiling
// costs a real joiner nothing.
func TestEnrollRefusesAFloodOfAttempts(t *testing.T) {
	t.Parallel()
	handler, _ := enrollFixture(t)

	var refusedForRate int
	for range enrollBurst + 20 {
		_, err := handler(context.Background(), &gastrologv1.EnrollRequest{
			TokenSecret: "wrong",
			NodeId:      []byte("attacker"),
		})
		if err != nil && strings.Contains(err.Error(), "rate") {
			refusedForRate++
		}
	}
	if refusedForRate == 0 {
		t.Fatal("every attempt in a flood was answered; nothing bounds guessing")
	}
}

// The ceiling must not stop the one caller it exists to protect: a node
// joining for the first time gets through.
func TestEnrollBurstAdmitsALegitimateJoiner(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	if _, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: secret,
		NodeId:      []byte("node-2"),
		CsrPem:      csrFor(t, "node-2"),
	}); err != nil {
		t.Fatalf("the first joiner was refused: %v", err)
	}
}
