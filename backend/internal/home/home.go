// Package home manages the gastrolog home directory layout.
//
// The home directory owns all persistent state: config files, user databases,
// and per-vault chunk/index directories.
//
// Layout:
//
//	<root>/
//	  config.json   or  config.db     (config store, type-dependent)
//	  users.json                       (user credentials, JSON file store only)
//	  raft/
//	    raft.db                        (boltdb: raft log + stable store)
//	    snapshots/                     (raft file snapshot store)
//	  stores/
//	    <vault-id>/                    (per-vault chunk + index data)
package home

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

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

// UsersPath returns the path to the users JSON file.
func (d Dir) UsersPath() string {
	return filepath.Join(d.root, "users.json")
}

// VaultDir returns the directory for a specific vault's chunk/index data.
// The on-disk path is "stores/" for backward compatibility with existing data.
func (d Dir) VaultDir(vaultID string) string {
	return filepath.Join(d.root, "stores", vaultID)
}

// RaftDir returns the directory for Raft persistent state (log store, snapshots).
func (d Dir) RaftDir() string {
	return filepath.Join(d.root, "raft")
}

// LookupDir returns the directory for auto-downloaded lookup databases (e.g. MaxMind MMDB files).
func (d Dir) LookupDir() string {
	return filepath.Join(d.root, "lookups")
}

// EnsureExists creates the home directory (and parents) if it doesn't exist.
func (d Dir) EnsureExists() error {
	if err := os.MkdirAll(d.root, 0o750); err != nil {
		return fmt.Errorf("create home directory %s: %w", d.root, err)
	}
	return nil
}

// NodeID reads the persistent node identity from <root>/node_id.
// If the file doesn't exist, a new UUIDv7 is generated and written.
func (d Dir) NodeID() (string, error) {
	return d.readOrCreate("node_id", func() string {
		return uuid.Must(uuid.NewV7()).String()
	})
}

// readOrCreate reads a single-line value from <root>/<filename>.
// If the file doesn't exist, generate() provides the default which is persisted.
func (d Dir) readOrCreate(filename string, generate func() string) (string, error) {
	p := filepath.Join(d.root, filename)
	data, err := os.ReadFile(p) //nolint:gosec // G304: path is constructed from trusted home dir + constant filename
	if err == nil {
		if v := strings.TrimSpace(string(data)); v != "" {
			return v, nil
		}
	}
	v := generate()
	if err := os.WriteFile(p, []byte(v+"\n"), 0o640); err != nil { //nolint:gosec // G306: node-id file is not secret, 0640 is intentional
		return "", fmt.Errorf("write %s: %w", filename, err)
	}
	return v, nil
}
