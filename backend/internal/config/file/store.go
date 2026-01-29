// Package file provides a file-based ConfigStore implementation.
package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gastrolog/internal/config"
)

// Store is a file-based ConfigStore implementation.
// Configuration is persisted as JSON for human readability.
// Writes are atomic via temp file + rename.
type Store struct {
	path string
}

// NewStore creates a new file-based ConfigStore.
// The path specifies where the configuration file will be stored.
func NewStore(path string) *Store {
	return &Store{path: path}
}

// Load reads the configuration from disk.
// Returns nil config if the file does not exist.
func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg config.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	return &cfg, nil
}

// Save persists the configuration to disk.
// Uses atomic write via temp file + rename.
func (s *Store) Save(ctx context.Context, cfg *config.Config) error {
	// Ensure directory exists.
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	// Marshal to JSON with indentation for readability.
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	// Write to temp file.
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, s.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename config file: %w", err)
	}

	return nil
}
