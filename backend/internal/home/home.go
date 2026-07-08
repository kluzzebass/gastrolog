// Package home manages the gastrolog home directory layout.
//
// The home directory owns all persistent state: config files, user databases,
// and per-vault chunk/index directories.
//
// Layout:
//
//	<root>/
//	  node_id                          (advisory cache — see app.resolveNodeID; the raft StableStore is canonical)
//	  node_name                        (human-readable petname, mirrors config store)
//	  raft/
//	    wal/                           (cluster-ctl raftwal: log + stable store only)
//	    groups/
//	      wal/                         (vault-ctl multiraft raftwal: batched fsync across vault groups)
//	      cluster-ctl/                 (cluster-ctl raft file snapshots)
//	      <vault-ctl-group-id>/        (vault metadata raft snapshots)
//	  stores/
//	    <vault-id>/                    (per-vault chunk + index data)
//	  segments/
//	    <vault-id>/                    (pipeline segment working + completed areas)
//	  managed-files/
//	    <file-id>/                     (managed file entity: lookups, etc.)
package home

import (
	"fmt"
	"gastrolog/internal/glid"
	"os"
	"path/filepath"
	"strings"
)

// readSmall reads a small identity file (node_id, node_name) with a bounded
// buffer. These are tiny constant-size files (< 64 bytes) read once at
// startup — mmap is slower here (9.9µs vs 8.4µs, benchmarked), and the
// stdlib slurp function over-allocates (920 B vs 200 B).
func readSmall(path string) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec // G304: paths from trusted home dir + constant filename
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 256)
	n, _ := f.Read(buf)
	return buf[:n], nil
}

// Dir represents a gastrolog home directory.
type Dir struct {
	root string
}

// New creates a Dir with an explicit root path.
func New(root string) Dir {
	return Dir{root: root}
}

// Default returns a Dir using the platform-appropriate default location:
//   - Linux:   ~/.config/gastrolog
//   - macOS:   ~/Library/Application Support/gastrolog
//   - Windows: %APPDATA%/gastrolog
func Default() (Dir, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return Dir{}, fmt.Errorf("determine config directory: %w", err)
	}
	return Dir{root: filepath.Join(base, "gastrolog")}, nil
}

// Root returns the home directory path.
func (d Dir) Root() string {
	return d.root
}

// VaultDir returns the directory for a specific vault's chunk/index data.
// The on-disk path is "stores/" for backward compatibility with existing data.
func (d Dir) VaultDir(vaultID string) string {
	return filepath.Join(d.root, "stores", vaultID)
}

// SegmentsDir returns the base directory for pipeline segment storage.
// Each vault's origin root is SegmentsDir/<vault-id>/ (working/, completed/, chunks/).
func (d Dir) SegmentsDir() string {
	return filepath.Join(d.root, "segments")
}

// RaftDir returns the directory for Raft persistent state (log store, snapshots).
func (d Dir) RaftDir() string {
	return filepath.Join(d.root, "raft")
}

// ClusterCtlWALDir returns the on-disk directory for the cluster-ctl raftwal.
// Isolated from vault groups so cluster heartbeats are not coupled to vault
// append/fsync batching (gastrolog-3tp89).
func (d Dir) ClusterCtlWALDir() string {
	return filepath.Join(d.RaftDir(), "wal")
}

// VaultCtlWALDir returns the on-disk directory for vault control-plane raftwal.
// All vault/…/ctl groups on this node share this WAL (coalesced fsync).
func (d Dir) VaultCtlWALDir() string {
	return filepath.Join(d.RaftDir(), "groups", "wal")
}

// RaftGroupDir returns the per-group directory under raft/groups/<groupID>/.
// Used for file snapshot stores: "system" for cluster config raft, vault-instance GLID
// strings for vault metadata raft (see raftgroup.GroupManager BaseDir).
func (d Dir) RaftGroupDir(groupID string) string {
	return filepath.Join(d.RaftDir(), "groups", groupID)
}

// ClusterTLSPath returns the path to the local cluster TLS material file.
// This file persists mTLS certs outside of Raft so they're available on
// restart before Raft can communicate with peers.
func (d Dir) ClusterTLSPath() string {
	return filepath.Join(d.root, "cluster-tls.json")
}

// ManagedFilesDir returns the directory for uploaded managed files.
func (d Dir) ManagedFilesDir() string {
	return filepath.Join(d.root, "managed-files")
}

const managedFilesDir = "managed-files"
const managedFileDataName = "data"

// ManagedFileDir returns the directory for a specific managed file.
func (d Dir) ManagedFileDir(fileID string) string {
	return filepath.Join(d.root, managedFilesDir, fileID)
}

// ManagedFilePath returns the canonical on-disk path for a managed file's data.
// This is the single source of truth — all code that reads or writes managed
// file data must use this function.
func (d Dir) ManagedFilePath(fileID string) string {
	return filepath.Join(d.root, managedFilesDir, fileID, managedFileDataName)
}

// SocketPath returns the path to the Unix domain socket for local CLI access.
func (d Dir) SocketPath() string {
	return filepath.Join(d.root, "gastrolog.sock")
}

// EnsureExists creates the home directory (and parents) if it doesn't exist.
func (d Dir) EnsureExists() error {
	if err := os.MkdirAll(d.root, 0o750); err != nil {
		return fmt.Errorf("create home directory %s: %w", d.root, err)
	}
	return nil
}

// WriteNodeIDFile writes the advisory node_id file. Called after the
// canonical ID is resolved from (or written to) the raft StableStore so the
// file always mirrors the true identity. Safe to overwrite.
func (d Dir) WriteNodeIDFile(id glid.GLID) error {
	p := filepath.Join(d.root, "node_id")
	if err := os.WriteFile(p, []byte(id.String()+"\n"), 0o640); err != nil { //nolint:gosec // G306: node_id file is not secret
		return fmt.Errorf("write node_id: %w", err)
	}
	return nil
}

// ReadNodeName reads the cached node name from <root>/node_name.
// Returns empty string if the file doesn't exist yet.
func (d Dir) ReadNodeName() string {
	data, err := readSmall(filepath.Join(d.root, "node_name"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// WriteNodeName writes the node's human-readable name to <root>/node_name.
// The config store is the source of truth; this file is a convenience for
// operators inspecting the home directory on disk.
func (d Dir) WriteNodeName(name string) error {
	p := filepath.Join(d.root, "node_name")
	return os.WriteFile(p, []byte(name+"\n"), 0o640) //nolint:gosec // G306: node_name is not secret
}
