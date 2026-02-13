package cert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func genCertAndKey(t *testing.T, certPath, keyPath string) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}))
	if certPath != "" && keyPath != "" {
		if err := os.WriteFile(certPath, []byte(certPEM), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, []byte(keyPEM), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return certPEM, keyPEM
}

func TestManager_AddFromPEMAndCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certPEM, keyPEM := genCertAndKey(t, certPath, keyPath)

	mgr := New(Config{Logger: slog.Default()})
	if err := mgr.AddFromPEM("server", certPEM, keyPEM); err != nil {
		t.Fatal(err)
	}
	mgr.SetDefault("server")

	c := mgr.Certificate("server")
	if c == nil {
		t.Fatal("expected certificate")
	}
	if len(c.Certificate) == 0 {
		t.Fatal("certificate has no chain")
	}

	// GetCertificate with empty SNI returns default
	got, err := mgr.GetCertificate(&tls.ClientHelloInfo{ServerName: ""})
	if err != nil {
		t.Fatal(err)
	}
	if got != c {
		t.Fatal("GetCertificate(empty SNI) should return default cert")
	}
}

func TestManager_LoadFromConfig(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certPEM, keyPEM := genCertAndKey(t, certPath, keyPath)

	mgr := New(Config{Logger: slog.Default()})
	certs := map[string]CertSource{
		"server": {CertPEM: certPEM, KeyPEM: keyPEM},
	}
	if err := mgr.LoadFromConfig("server", certs); err != nil {
		t.Fatal(err)
	}

	c := mgr.Certificate("server")
	if c == nil {
		t.Fatal("expected certificate")
	}
}

func TestManager_LoadFromConfig_FilePaths(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	genCertAndKey(t, certPath, keyPath)

	mgr := New(Config{Logger: slog.Default()})
	certs := map[string]CertSource{
		"server": {CertFile: certPath, KeyFile: keyPath},
	}
	if err := mgr.LoadFromConfig("server", certs); err != nil {
		t.Fatal(err)
	}

	c := mgr.Certificate("server")
	if c == nil {
		t.Fatal("expected certificate")
	}
}
