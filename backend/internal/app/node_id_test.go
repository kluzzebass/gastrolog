package app

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

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
	walDir := filepath.Join(hd.RaftDir(), "wal")
	wal, err := raftwal.Open(walDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = wal.Close() }()
	gs := wal.GroupStore("system")
	return gs.Get([]byte(nodeIDKey))
}

func TestResolveNodeID_FreshDirectoryGeneratesAndPersists(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	id, err := resolveNodeID(hd, logger)
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
	fileID, err := hd.ReadNodeIDFile()
	if err != nil {
		t.Fatalf("ReadNodeIDFile: %v", err)
	}
	if fileID != id {
		t.Fatalf("advisory file id = %s, want %s", fileID, id)
	}
}

func TestResolveNodeID_IdempotentAcrossCalls(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	first, err := resolveNodeID(hd, logger)
	if err != nil {
		t.Fatalf("first resolveNodeID: %v", err)
	}
	second, err := resolveNodeID(hd, logger)
	if err != nil {
		t.Fatalf("second resolveNodeID: %v", err)
	}
	if first != second {
		t.Fatalf("identity drifted: first=%s second=%s", first, second)
	}
}

func TestResolveNodeID_MigratesFromLegacyFile(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	// Pre-populate a legacy <home>/node_id file as if from gastrolog-25z9.
	legacyID := glid.New()
	if err := hd.WriteNodeIDFile(legacyID); err != nil {
		t.Fatalf("WriteNodeIDFile: %v", err)
	}

	got, err := resolveNodeID(hd, logger)
	if err != nil {
		t.Fatalf("resolveNodeID: %v", err)
	}
	if got != legacyID {
		t.Fatalf("expected legacy ID %s, got %s", legacyID, got)
	}

	// StableStore now holds the migrated value.
	raw, err := readStableStoreNodeID(t, hd)
	if err != nil {
		t.Fatalf("readStableStoreNodeID: %v", err)
	}
	if glid.FromBytes(raw) != legacyID {
		t.Fatalf("stablestore id = %s, want %s (migrated)", glid.FromBytes(raw), legacyID)
	}

	// Subsequent call reads from StableStore (not the file).
	got2, err := resolveNodeID(hd, logger)
	if err != nil {
		t.Fatalf("second resolveNodeID: %v", err)
	}
	if got2 != legacyID {
		t.Fatalf("second resolve drifted: %s", got2)
	}
}

func TestResolveNodeID_StableStoreOverridesWipedFile(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	id, err := resolveNodeID(hd, logger)
	if err != nil {
		t.Fatalf("resolveNodeID: %v", err)
	}

	// Operator deletes the advisory file — runtime must not treat this as
	// identity loss. On next resolve the file gets rewritten from the
	// StableStore value.
	if err := os.Remove(filepath.Join(hd.Root(), "node_id")); err != nil {
		t.Fatalf("remove advisory file: %v", err)
	}

	got, err := resolveNodeID(hd, logger)
	if err != nil {
		t.Fatalf("second resolveNodeID: %v", err)
	}
	if got != id {
		t.Fatalf("identity regenerated after file wipe: first=%s got=%s", id, got)
	}

	// File was restored.
	fileID, err := hd.ReadNodeIDFile()
	if err != nil {
		t.Fatalf("ReadNodeIDFile after resolve: %v", err)
	}
	if fileID != id {
		t.Fatalf("advisory file id = %s, want %s (restored)", fileID, id)
	}
}

func TestResolveNodeID_RejectsCorruptStableStoreValue(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	logger := nodeIDTestLogger()

	// Seed the StableStore with a wrong-size blob.
	walDir := filepath.Join(hd.RaftDir(), "wal")
	if err := os.MkdirAll(walDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	wal, err := raftwal.Open(walDir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	gs := wal.GroupStore("system")
	if err := gs.Set([]byte(nodeIDKey), []byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	_, err = resolveNodeID(hd, logger)
	if err == nil {
		t.Fatal("expected error for corrupt stablestore value, got nil")
	}
}

func TestReadNodeIDFile_MissingReturnsNotExist(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	_, err := hd.ReadNodeIDFile()
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}
