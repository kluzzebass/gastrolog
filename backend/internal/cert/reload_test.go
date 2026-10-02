package cert

// The manager has exactly one fill path, and it keys by NAME: names are
// what every consumer resolves (ingester tls params, the default serving
// certificate). Keying the boot fill by ID while the runtime reload keyed by
// name is how a restart broke every name lookup until the next edit.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"

	"gastrolog/internal/glid"
)

func seedCert(t *testing.T, store system.Store, name string) glid.GLID {
	t.Helper()
	certPEM, keyPEM := selfSignedPair(t)
	id := glid.New()
	if err := store.PutCertificate(context.Background(), system.CertPEM{
		ID: id, Name: name, CertPEM: certPEM, KeyPEM: keyPEM,
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestReloadFromStoreKeysByName(t *testing.T) {
	t.Parallel()
	store := sysmem.NewStore()
	seedCert(t, store, "syslog-srv")

	mgr := New(Config{})
	if err := ReloadFromStore(context.Background(), mgr, store); err != nil {
		t.Fatal(err)
	}
	if mgr.Certificate("syslog-srv") == nil {
		t.Fatal("a boot-time fill cannot resolve the certificate by name")
	}
}

func TestReloadFromStorePropagatesDeletes(t *testing.T) {
	t.Parallel()
	store := sysmem.NewStore()
	id := seedCert(t, store, "doomed")

	mgr := New(Config{})
	if err := ReloadFromStore(context.Background(), mgr, store); err != nil {
		t.Fatal(err)
	}
	if mgr.Certificate("doomed") == nil {
		t.Fatal("seeded certificate absent")
	}
	if err := store.DeleteCertificate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := ReloadFromStore(context.Background(), mgr, store); err != nil {
		t.Fatal(err)
	}
	if mgr.Certificate("doomed") != nil {
		t.Fatal("a deleted certificate survived the reload")
	}
}

func TestReloadFromStoreFallsBackToIDForUnnamed(t *testing.T) {
	t.Parallel()
	store := sysmem.NewStore()
	certPEM, keyPEM := selfSignedPair(t)
	id := glid.New()
	if err := store.PutCertificate(context.Background(), system.CertPEM{
		ID: id, CertPEM: certPEM, KeyPEM: keyPEM,
	}); err != nil {
		t.Fatal(err)
	}
	mgr := New(Config{})
	if err := ReloadFromStore(context.Background(), mgr, store); err != nil {
		t.Fatal(err)
	}
	if mgr.Certificate(id.String()) == nil {
		t.Fatal("an unnamed certificate is unreachable")
	}
}

// selfSignedPair mints a minimal valid certificate and key.
func selfSignedPair(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "t"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}
