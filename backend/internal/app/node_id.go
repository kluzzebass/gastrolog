package app

import (
	"errors"
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
// human-readable advisory cache.
//
// Resolution order:
//  1. StableStore has the key → decode and use it
//  2. StableStore empty + <home>/node_id exists → migrate (one-shot for clusters
//     upgraded from gastrolog-25z9 where the file WAS canonical)
//  3. StableStore empty + no legacy file → generate a new GLID
//
// The StableStore is written synchronously (fsync'd via raftwal's batch writer)
// before returning. The advisory file is also (re)written so operators can
// still `cat data/node1/node_id` to see the ID.
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
	if len(raw) == glid.Size {
		id := glid.FromBytes(raw)
		writeAdvisoryNodeID(hd, id, logger)
		return id, nil
	}
	if len(raw) != 0 {
		return glid.GLID{}, fmt.Errorf("stablestore node_id is %d bytes, expected %d", len(raw), glid.Size)
	}

	// Not in StableStore — try the legacy file (migration path).
	id, err := hd.ReadNodeIDFile()
	switch {
	case err == nil:
		logger.Warn("migrating node_id from legacy home file to raft StableStore — file is now advisory",
			"node_id", id.String())
	case errors.Is(err, os.ErrNotExist):
		id = glid.New()
		logger.Info("generated new node_id", "node_id", id.String())
	default:
		return glid.GLID{}, fmt.Errorf("read legacy node_id file: %w", err)
	}

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
