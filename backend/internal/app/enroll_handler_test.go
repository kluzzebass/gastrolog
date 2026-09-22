package app

// Enrolment is the one call that hands out the cluster's TLS material in
// exchange for a secret, so what it does with a wrong secret matters as much
// as what it does with the right one.

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// enrollFixture builds a store holding real cluster TLS material and returns
// the handler plus the join secret a legitimate joiner would present.
func enrollFixture(t *testing.T) (handler func(context.Context, *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error), secret string) {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	crt, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, nil)
	if err != nil {
		t.Fatal(err)
	}
	token, err := tlsutil.GenerateJoinToken(ca.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err = tlsutil.ParseJoinToken(token)
	if err != nil {
		t.Fatal(err)
	}

	store := sysmem.NewStore()
	if err := store.PutClusterTLS(context.Background(), system.ClusterTLS{
		CACertPEM:      string(ca.CertPEM),
		CAKeyPEM:       string(ca.KeyPEM),
		ClusterCertPEM: string(crt.CertPEM),
		ClusterKeyPEM:  string(crt.KeyPEM),
		JoinToken:      token,
	}); err != nil {
		t.Fatal(err)
	}
	return makeEnrollHandler(store, slog.New(slog.DiscardHandler)), secret
}

func TestEnrollAcceptsTheRealSecret(t *testing.T) {
	t.Parallel()
	handler, secret := enrollFixture(t)

	resp, err := handler(context.Background(), &gastrologv1.EnrollRequest{
		TokenSecret: secret,
		NodeId:      []byte("node-2"),
		NodeAddr:    "node-2:4566",
	})
	if err != nil {
		t.Fatalf("a joiner with the real token was refused: %v", err)
	}
	if len(resp.GetClusterKeyPem()) == 0 || len(resp.GetCaCertPem()) == 0 {
		t.Fatal("enrolment returned no TLS material")
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
	}); err == nil {
		t.Fatal("a wrong token was accepted")
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
	}); err != nil {
		t.Fatalf("the first joiner was refused: %v", err)
	}
}
