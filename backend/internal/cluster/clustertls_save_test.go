package cluster

// The file this writes holds the node's cluster private key. Writing it
// through a fixed temp name lets anyone who can create a path in that
// directory decide where the key lands.

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/cluster/tlsutil"
)

func TestSaveFileWritesOwnerOnly(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	crt, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, LaneSANs)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "cluster-tls.json")
	if err := SaveFile(path, crt.CertPEM, crt.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("SaveFile: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("the cluster key file has mode %04o; group and other must have nothing", mode)
	}
}

// A name an attacker can guess is a name they can occupy first, pointing
// the write wherever they like.
func TestSaveFileDoesNotUseAGuessableTempName(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	crt, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, LaneSANs)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "cluster-tls.json")

	// The name the old implementation used. Occupying it must not divert or
	// fail the write.
	squatted := path + ".tmp"
	elsewhere := filepath.Join(dir, "captured.json")
	if err := os.Symlink(elsewhere, squatted); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if err := SaveFile(path, crt.CertPEM, crt.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("SaveFile: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the key file was not written where it was asked: %v", err)
	}
	if _, err := os.Lstat(elsewhere); err == nil {
		t.Error("the key was written through the planted symlink")
	}
}

// Saving twice must not trip over its own leftovers.
func TestSaveFileIsRepeatable(t *testing.T) {
	t.Parallel()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	crt, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, LaneSANs)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "cluster-tls.json")
	for range 3 {
		if err := SaveFile(path, crt.CertPEM, crt.KeyPEM, ca.CertPEM); err != nil {
			t.Fatalf("SaveFile: %v", err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("%d files left behind, want just the key file: %v", len(entries), names)
	}

	ctls := NewClusterTLS()
	found, err := ctls.LoadFile(path)
	if err != nil || !found {
		t.Fatalf("the saved file does not load back: found=%v err=%v", found, err)
	}
}
