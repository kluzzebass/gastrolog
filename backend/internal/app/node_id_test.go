package app

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/raftwal"
)

func newTestHome(t *testing.T) home.Dir {
	t.Helper()
	hd := home.New(t.TempDir())
	if err := hd.EnsureExists(); err != nil {
		t.Fatalf("EnsureExists: %v", err)
	}
	return hd
}

func nodeIDTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// readStableStoreNodeID opens the WAL and reads the node_id key directly.
// Used by tests to verify that resolveNodeID persisted the value.
func readStableStoreNodeID(t *testing.T, hd home.Dir) ([]byte, error) {
	t.Helper()
	wal, err := raftwal.Open(hd.ClusterCtlWALDir())
	if err != nil {
		return nil, err
	}
	defer func() { _ = wal.Close() }()
	gs := wal.GroupStore("cluster-ctl")
	return gs.Get([]byte(nodeIDKey))
}

// readAdvisoryFile reads <home>/node_id and parses it as a GLID. The runtime
// never reads this file (StableStore is canonical); tests inspect it only to
// verify the advisory cache was refreshed.
func readAdvisoryFile(t *testing.T, hd home.Dir) glid.GLID {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(hd.Root(), "node_id"))
	if err != nil {
		t.Fatalf("read advisory file: %v", err)
	}
	id, err := glid.Parse(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parse advisory file: %v", err)
	}
	return id
}

func TestResolveNodeID_FreshDirectoryGeneratesAndPersists(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	ident, err := resolveNodeID(hd, logger)
	id := ident.ID
	if err != nil {
		t.Fatalf("resolveNodeID: %v", err)
	}
	if id.IsZero() {
		t.Fatal("expected non-zero GLID")
	}

	// StableStore now holds the same ID as raw 16 bytes.
	raw, err := readStableStoreNodeID(t, hd)
	if err != nil {
		t.Fatalf("readStableStoreNodeID: %v", err)
	}
	if len(raw) != glid.Size {
		t.Fatalf("stablestore has %d bytes, want %d", len(raw), glid.Size)
	}
	if glid.FromBytes(raw) != id {
		t.Fatalf("stablestore id = %s, want %s", glid.FromBytes(raw), id)
	}

	// Advisory file mirrors the ID.
	if got := readAdvisoryFile(t, hd); got != id {
		t.Fatalf("advisory file id = %s, want %s", got, id)
	}
}

func TestResolveNodeID_IdempotentAcrossCalls(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	firstIdentity, err := resolveNodeID(hd, logger)
	first := firstIdentity.ID
	if err != nil {
		t.Fatalf("first resolveNodeID: %v", err)
	}
	secondIdentity, err := resolveNodeID(hd, logger)
	second := secondIdentity.ID
	if err != nil {
		t.Fatalf("second resolveNodeID: %v", err)
	}
	if first != second {
		t.Fatalf("identity drifted: first=%s second=%s", first, second)
	}
}

func TestResolveNodeID_StableStoreOverridesWipedFile(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	ident, err := resolveNodeID(hd, logger)
	id := ident.ID
	if err != nil {
		t.Fatalf("resolveNodeID: %v", err)
	}

	// Operator deletes the advisory file — runtime must not treat this as
	// identity loss. On next resolve the file gets rewritten from the
	// StableStore value, not regenerated.
	if err := os.Remove(filepath.Join(hd.Root(), "node_id")); err != nil {
		t.Fatalf("remove advisory file: %v", err)
	}

	gotIdentity, err := resolveNodeID(hd, logger)
	got := gotIdentity.ID
	if err != nil {
		t.Fatalf("second resolveNodeID: %v", err)
	}
	if got != id {
		t.Fatalf("identity regenerated after file wipe: first=%s got=%s", id, got)
	}

	if restored := readAdvisoryFile(t, hd); restored != id {
		t.Fatalf("advisory file id = %s, want %s (restored)", restored, id)
	}
}

func TestResolveNodeID_StaleAdvisoryFileIgnored(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	// Resolve once so StableStore has the canonical value.
	ident, err := resolveNodeID(hd, logger)
	id := ident.ID
	if err != nil {
		t.Fatalf("resolveNodeID: %v", err)
	}

	// Overwrite the advisory file with an unrelated GLID. The runtime must
	// keep the StableStore value and overwrite the stale file on next resolve.
	stale := glid.New()
	if stale == id {
		t.Fatal("test precondition: stale GLID collided with canonical")
	}
	if err := hd.WriteNodeIDFile(stale); err != nil {
		t.Fatalf("WriteNodeIDFile: %v", err)
	}

	gotIdentity, err := resolveNodeID(hd, logger)
	got := gotIdentity.ID
	if err != nil {
		t.Fatalf("second resolveNodeID: %v", err)
	}
	if got != id {
		t.Fatalf("stale file won: got=%s, want canonical=%s", got, id)
	}
	if rewritten := readAdvisoryFile(t, hd); rewritten != id {
		t.Fatalf("advisory file still stale: got=%s, want=%s", rewritten, id)
	}
}

func TestResolveNodeID_RejectsCorruptStableStoreValue(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	// Seed the StableStore with a wrong-size blob.
	walDir := hd.ClusterCtlWALDir()
	if err := os.MkdirAll(walDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	wal, err := raftwal.Open(walDir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	gs := wal.GroupStore("cluster-ctl")
	if err := gs.Set([]byte(nodeIDKey), []byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	if _, err := resolveNodeID(hd, logger); err == nil {
		t.Fatal("expected error for corrupt stablestore value, got nil")
	}
}

// withClusterCtlStore runs fn against the home's identity store directly.
func withClusterCtlStore(t *testing.T, hd home.Dir, fn func(gs *raftwal.GroupStore)) {
	t.Helper()
	if err := os.MkdirAll(hd.ClusterCtlWALDir(), 0o750); err != nil {
		t.Fatal(err)
	}
	wal, err := raftwal.Open(hd.ClusterCtlWALDir())
	if err != nil {
		t.Fatal(err)
	}
	fn(wal.GroupStore("cluster-ctl"))
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
}

// A removal recorded beside the identity comes back with it on every
// resolution, and the identity stays the one that was removed.
func TestResolveNodeID_ReturnsTheRecordedRemoval(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	first, err := resolveNodeID(hd, logger)
	if err != nil {
		t.Fatal(err)
	}
	if first.Removal != nil {
		t.Fatalf("a fresh identity reports removal %+v", first.Removal)
	}
	removal := nodeRemoval{At: time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC), Reason: "removed from cluster by leader"}
	withClusterCtlStore(t, hd, func(gs *raftwal.GroupStore) {
		if err := recordNodeRemoval(gs, removal); err != nil {
			t.Fatal(err)
		}
	})

	for range 2 {
		got, err := resolveNodeID(hd, logger)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != first.ID {
			t.Fatalf("identity %s after removal, want %s", got.ID, first.ID)
		}
		if got.Removal == nil || !got.Removal.At.Equal(removal.At) || got.Removal.Reason != removal.Reason {
			t.Fatalf("removal %+v, want %+v", got.Removal, removal)
		}
	}
}

func TestResolveNodeID_RejectsCorruptRemoval(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	if _, err := resolveNodeID(hd, nodeIDTestLogger()); err != nil {
		t.Fatal(err)
	}
	withClusterCtlStore(t, hd, func(gs *raftwal.GroupStore) {
		if err := gs.Set([]byte(nodeRemovalKey), []byte("{not json")); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := resolveNodeID(hd, nodeIDTestLogger()); err == nil {
		t.Fatal("a corrupt removal record resolved")
	}
}

// A home that lost its identity store but kept its certificate is not
// pristine: minting there would pair a new identity with a certificate that
// names another. The certificate need not even be readable.
func TestResolveNodeID_RefusesACertificateWithoutItsIdentity(t *testing.T) {
	t.Parallel()
	for name, content := range map[string]string{
		"loadable":   `{"ca_cert_pem":"","cluster_cert_pem":"","cluster_key_pem":""}`,
		"unreadable": "\x00",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			hd := newTestHome(t)
			if err := os.WriteFile(hd.ClusterTLSPath(), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := resolveNodeID(hd, nodeIDTestLogger())
			if !errors.Is(err, errIncompleteHome) {
				t.Fatalf("resolveNodeID: %v, want the home refused as incomplete", err)
			}
			if raw, err := readStableStoreNodeID(t, hd); err != nil || len(raw) != 0 {
				t.Fatalf("an identity was minted beside the certificate: %x (%v)", raw, err)
			}
		})
	}
}
