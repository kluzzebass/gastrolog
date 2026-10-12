package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/raftwal"
)

// nodeIDKey is the reserved StableStore key under which each node persists
// its canonical 16-byte GLID identity. Prefixed to avoid collision with
// hraft's own StableStore keys (CurrentTerm, LastVoteCand, LastVoteTerm).
const nodeIDKey = "gastrolog:node_id"

// nodeRemovalKey is the StableStore key holding this node's removal, beside
// the identity it retires: deleting one without the other means deleting the
// cluster-ctl WAL, which takes the identity with it.
const nodeRemovalKey = "gastrolog:node_removal"

// errIncompleteHome refuses a home that holds a cluster certificate but no
// node identity. Minting an identity there would pair a new node ID with a
// certificate naming another, which no member admits.
var errIncompleteHome = errors.New("home holds a cluster certificate but no node identity")

// nodeIdentity is what a home says about the node it belongs to.
type nodeIdentity struct {
	ID glid.GLID
	// Removal is set once the cluster has removed this node. The identity
	// is retired for good: the node takes no further part in the cluster.
	Removal *nodeRemoval
}

// nodeRemoval is what a node knows about its own removal from the cluster.
type nodeRemoval struct {
	At     time.Time `json:"removed_at"`
	Reason string    `json:"reason"`
}

// stableStore is the slice of a raftwal.GroupStore the identity keys need.
type stableStore interface {
	Get(key []byte) ([]byte, error)
	Set(key, val []byte) error
}

// resolveNodeID returns the canonical node identity for a non-memory config.
// The system-raft StableStore is the source of truth; <home>/node_id is a
// human-readable advisory cache, rewritten after each resolution.
//
// On a pristine data directory a new GLID is generated and persisted to
// StableStore before returning. On subsequent boots the StableStore is read.
// There is no silent regeneration path — once StableStore has the key, it
// wins; a corrupt value is an error, not an excuse to mint new identity. Nor
// is a home that already holds a cluster certificate pristine: its
// certificate names the identity it lost.
func resolveNodeID(hd home.Dir, logger *slog.Logger) (nodeIdentity, error) {
	walDir := hd.ClusterCtlWALDir()
	if err := os.MkdirAll(walDir, 0o750); err != nil {
		return nodeIdentity{}, fmt.Errorf("create cluster-ctl raft wal dir: %w", err)
	}

	wal, err := raftwal.Open(walDir)
	if err != nil {
		return nodeIdentity{}, fmt.Errorf("open cluster-ctl raft WAL for node_id peek: %w", err)
	}
	defer func() { _ = wal.Close() }()

	gs := wal.GroupStore("cluster-ctl")

	raw, err := gs.Get([]byte(nodeIDKey))
	if err != nil {
		return nodeIdentity{}, fmt.Errorf("read node_id key: %w", err)
	}
	switch {
	case len(raw) == glid.Size:
		id := glid.FromBytes(raw)
		removal, err := readNodeRemoval(gs)
		if err != nil {
			return nodeIdentity{}, err
		}
		writeAdvisoryNodeID(hd, id, logger)
		return nodeIdentity{ID: id, Removal: removal}, nil
	case len(raw) != 0:
		return nodeIdentity{}, fmt.Errorf("stablestore node_id is %d bytes, expected %d", len(raw), glid.Size)
	}

	if err := requireNoCertificate(hd); err != nil {
		return nodeIdentity{}, err
	}

	// Pristine data dir: mint and persist.
	id := glid.New()
	logger.Info("generated new node_id", "node_id", id.String())
	if err := gs.Set([]byte(nodeIDKey), id[:]); err != nil {
		return nodeIdentity{}, fmt.Errorf("persist node_id to stablestore: %w", err)
	}
	writeAdvisoryNodeID(hd, id, logger)
	return nodeIdentity{ID: id}, nil
}

// requireNoCertificate refuses to treat a home as pristine while it holds a
// cluster certificate, readable or not.
func requireNoCertificate(hd home.Dir) error {
	_, err := os.Stat(hd.ClusterTLSPath())
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return fmt.Errorf("%w (%s): restore the raft directory this home lost, "+
		"or start this machine from an empty home directory to join as a new node",
		errIncompleteHome, hd.ClusterTLSPath())
}

// readNodeRemoval returns the removal recorded beside the node's identity,
// or nil while the node has not been removed.
func readNodeRemoval(gs stableStore) (*nodeRemoval, error) {
	raw, err := gs.Get([]byte(nodeRemovalKey))
	if err != nil {
		return nil, fmt.Errorf("read node removal key: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var r nodeRemoval
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("decode node removal: %w", err)
	}
	return &r, nil
}

// recordNodeRemoval persists the node's removal beside its identity, so the
// node comes back removed from every later start of this home.
func recordNodeRemoval(gs stableStore, r nodeRemoval) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode node removal: %w", err)
	}
	if err := gs.Set([]byte(nodeRemovalKey), raw); err != nil {
		return fmt.Errorf("persist node removal: %w", err)
	}
	return nil
}

// writeAdvisoryNodeID refreshes the human-readable file. Failures are
// non-fatal because the StableStore is authoritative.
func writeAdvisoryNodeID(hd home.Dir, id glid.GLID, logger *slog.Logger) {
	if err := hd.WriteNodeIDFile(id); err != nil {
		logger.Warn("advisory node_id file write failed (non-fatal)", "error", err)
	}
}
