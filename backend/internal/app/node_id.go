package app

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/raftwal"
)

// nodeIDKey is the reserved StableStore key under which each node persists
// its canonical 16-byte GLID identity. Prefixed to avoid collision with
// hraft's own StableStore keys (CurrentTerm, LastVoteCand, LastVoteTerm).
const nodeIDKey = "gastrolog:node_id"

// resolveNodeID returns the canonical node identity for a non-memory config.
// The system-raft StableStore is the source of truth; <home>/node_id is a
// human-readable advisory cache, rewritten after each resolution.
//
// On a pristine data directory a new GLID is generated and persisted to
// StableStore before returning. On subsequent boots the StableStore is read.
// There is no silent regeneration path — once StableStore has the key, it
// wins; a corrupt value is an error, not an excuse to mint new identity.
func resolveNodeID(hd home.Dir, logger *slog.Logger) (glid.GLID, error) {
	walDir := filepath.Join(hd.RaftDir(), "wal")
	if err := os.MkdirAll(walDir, 0o750); err != nil {
		return glid.GLID{}, fmt.Errorf("create raft wal dir: %w", err)
	}

	wal, err := raftwal.Open(walDir)
	if err != nil {
		return glid.GLID{}, fmt.Errorf("open system raft WAL for node_id peek: %w", err)
	}
	defer func() { _ = wal.Close() }()

	gs := wal.GroupStore("system")

	raw, err := gs.Get([]byte(nodeIDKey))
	if err != nil {
		return glid.GLID{}, fmt.Errorf("read node_id key: %w", err)
	}
	switch {
	case len(raw) == glid.Size:
		id := glid.FromBytes(raw)
		writeAdvisoryNodeID(hd, id, logger)
		return id, nil
	case len(raw) != 0:
		return glid.GLID{}, fmt.Errorf("stablestore node_id is %d bytes, expected %d", len(raw), glid.Size)
	}

	// Pristine data dir: mint and persist.
	id := glid.New()
	logger.Info("generated new node_id", "node_id", id.String())
	if err := gs.Set([]byte(nodeIDKey), id[:]); err != nil {
		return glid.GLID{}, fmt.Errorf("persist node_id to stablestore: %w", err)
	}
	writeAdvisoryNodeID(hd, id, logger)
	return id, nil
}

// writeAdvisoryNodeID refreshes the human-readable file. Failures are
// non-fatal because the StableStore is authoritative.
func writeAdvisoryNodeID(hd home.Dir, id glid.GLID, logger *slog.Logger) {
	if err := hd.WriteNodeIDFile(id); err != nil {
		logger.Warn("advisory node_id file write failed (non-fatal)", "error", err)
	}
}
