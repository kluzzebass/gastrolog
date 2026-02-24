// Package home manages the gastrolog home directory layout.
//
// The home directory owns all persistent state: config files, user databases,
// and per-store chunk/index directories.
//
// Layout:
//
//	<root>/
//	  config.json   or  config.db     (config store, type-dependent)
//	  users.json                       (user credentials, JSON file store only)
//	  stores/
//	    <store-id>/                    (per-store chunk + index data)
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

// ConfigPath returns the path to the config file for the given store type.
// "json" -> config.json, "sqlite" -> config.db.
func (d Dir) ConfigPath(storeType string) string {
	switch storeType {
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

// StoreDir returns the directory for a specific store's chunk/index data.
func (d Dir) StoreDir(storeID string) string {
	return filepath.Join(d.root, "stores", storeID)
}

// EnsureExists creates the home directory (and parents) if it doesn't exist.
func (d Dir) EnsureExists() error {
	if err := os.MkdirAll(d.root, 0o750); err != nil {
		return fmt.Errorf("create home directory %s: %w", d.root, err)
	}
	return nil
}
