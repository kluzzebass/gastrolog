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
//	  stores/
//	    <vault-id>/                    (per-vault chunk + index data)
package home

import (
	"fmt"
	"os"
	"path/filepath"
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

// ConfigPath returns the path to the config file for the given config type.
// "json" -> config.json, "sqlite" -> config.db.
func (d Dir) ConfigPath(configType string) string {
	switch configType {
	case "json":
		return filepath.Join(d.root, "config.json")
	default:
		return filepath.Join(d.root, "config.db")
	}
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
