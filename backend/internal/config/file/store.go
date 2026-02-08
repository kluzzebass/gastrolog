// Package file provides a file-based ConfigStore implementation.
//
// Configuration is persisted as a versioned JSON envelope:
//
//	{"version": 1, "config": { ... }}
//
// All mutations (Put/Delete) load the full file, mutate in memory, and
// atomically flush the entire file. This is the nature of JSON — every
// mutation rewrites the file.
package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/config"
)

const currentVersion = 1

// envelope is the versioned on-disk format.
type envelope struct {
	Version int            `json:"version"`
	Config  *config.Config `json:"config"`
}

// Store is a file-based ConfigStore implementation.
// Configuration is persisted as JSON for human readability.
// Writes are atomic via temp file + rename with round-trip validation.
type Store struct {
	path string
}

var _ config.Store = (*Store)(nil)

// NewStore creates a new file-based ConfigStore.
func NewStore(path string) *Store {
	return &Store{path: path}
}

// Load reads the full configuration from disk.
// Returns nil if the file does not exist.
func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// load reads and parses the config file. Returns nil,nil if not found.
func (s *Store) load() (*config.Config, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if env.Version == 0 {
		return nil, fmt.Errorf("unversioned config file detected; delete %s and restart to bootstrap a fresh config", s.path)
	}

	if env.Version > currentVersion {
		return nil, fmt.Errorf("config file version %d is newer than supported version %d", env.Version, currentVersion)
	}

	if env.Version < currentVersion {
		if err := migrateFile(s.path, data, env.Version); err != nil {
			return nil, fmt.Errorf("migrate config: %w", err)
		}
		// Re-read after migration.
		data, err = os.ReadFile(s.path)
		if err != nil {
			return nil, fmt.Errorf("read migrated config: %w", err)
		}
		if err := json.Unmarshal(data, &env); err != nil {
			return nil, fmt.Errorf("parse migrated config: %w", err)
		}
	}

	if env.Config == nil {
		return nil, nil
	}
	return env.Config, nil
}

// flush atomically writes the config to disk with round-trip validation.
func (s *Store) flush(cfg *config.Config) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	env := envelope{Version: currentVersion, Config: cfg}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	// Round-trip validation: re-read and verify valid JSON.
	check, err := os.ReadFile(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("read-back temp file: %w", err)
	}
	var verify envelope
	if err := json.Unmarshal(check, &verify); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("round-trip validation failed: %w", err)
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename config file: %w", err)
	}

	return nil
}

// loadOrEmpty loads the config, returning an empty Config if the file doesn't exist.
func (s *Store) loadOrEmpty() (*config.Config, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &config.Config{}
	}
	if cfg.Filters == nil {
		cfg.Filters = make(map[string]config.FilterConfig)
	}
	if cfg.RotationPolicies == nil {
		cfg.RotationPolicies = make(map[string]config.RotationPolicyConfig)
	}
	if cfg.RetentionPolicies == nil {
		cfg.RetentionPolicies = make(map[string]config.RetentionPolicyConfig)
	}
	if cfg.Settings == nil {
		cfg.Settings = make(map[string]string)
	}
	return cfg, nil
}

// Filters

func (s *Store) GetFilter(ctx context.Context, id string) (*config.FilterConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	fc, ok := cfg.Filters[id]
	if !ok {
		return nil, nil
	}
	return &fc, nil
}

func (s *Store) ListFilters(ctx context.Context) (map[string]config.FilterConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return make(map[string]config.FilterConfig), nil
	}
	if cfg.Filters == nil {
		return make(map[string]config.FilterConfig), nil
	}
	return cfg.Filters, nil
}

func (s *Store) PutFilter(ctx context.Context, id string, fc config.FilterConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	cfg.Filters[id] = fc
	return s.flush(cfg)
}

func (s *Store) DeleteFilter(ctx context.Context, id string) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	delete(cfg.Filters, id)
	return s.flush(cfg)
}

// Rotation policies

func (s *Store) GetRotationPolicy(ctx context.Context, id string) (*config.RotationPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	rp, ok := cfg.RotationPolicies[id]
	if !ok {
		return nil, nil
	}
	return &rp, nil
}

func (s *Store) ListRotationPolicies(ctx context.Context) (map[string]config.RotationPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return make(map[string]config.RotationPolicyConfig), nil
	}
	if cfg.RotationPolicies == nil {
		return make(map[string]config.RotationPolicyConfig), nil
	}
	return cfg.RotationPolicies, nil
}

func (s *Store) PutRotationPolicy(ctx context.Context, id string, rp config.RotationPolicyConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	cfg.RotationPolicies[id] = rp
	return s.flush(cfg)
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id string) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	delete(cfg.RotationPolicies, id)
	return s.flush(cfg)
}

// Retention policies

func (s *Store) GetRetentionPolicy(ctx context.Context, id string) (*config.RetentionPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	rp, ok := cfg.RetentionPolicies[id]
	if !ok {
		return nil, nil
	}
	return &rp, nil
}

func (s *Store) ListRetentionPolicies(ctx context.Context) (map[string]config.RetentionPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return make(map[string]config.RetentionPolicyConfig), nil
	}
	if cfg.RetentionPolicies == nil {
		return make(map[string]config.RetentionPolicyConfig), nil
	}
	return cfg.RetentionPolicies, nil
}

func (s *Store) PutRetentionPolicy(ctx context.Context, id string, rp config.RetentionPolicyConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	cfg.RetentionPolicies[id] = rp
	return s.flush(cfg)
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id string) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	delete(cfg.RetentionPolicies, id)
	return s.flush(cfg)
}

// Stores

func (s *Store) GetStore(ctx context.Context, id string) (*config.StoreConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	for _, st := range cfg.Stores {
		if st.ID == id {
			return &st, nil
		}
	}
	return nil, nil
}

func (s *Store) ListStores(ctx context.Context) ([]config.StoreConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	return cfg.Stores, nil
}

func (s *Store) PutStore(ctx context.Context, st config.StoreConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, existing := range cfg.Stores {
		if existing.ID == st.ID {
			cfg.Stores[i] = st
			return s.flush(cfg)
		}
	}
	cfg.Stores = append(cfg.Stores, st)
	return s.flush(cfg)
}

func (s *Store) DeleteStore(ctx context.Context, id string) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, st := range cfg.Stores {
		if st.ID == id {
			cfg.Stores = append(cfg.Stores[:i], cfg.Stores[i+1:]...)
			break
		}
	}
	return s.flush(cfg)
}

// Ingesters

func (s *Store) GetIngester(ctx context.Context, id string) (*config.IngesterConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	for _, ing := range cfg.Ingesters {
		if ing.ID == id {
			return &ing, nil
		}
	}
	return nil, nil
}

func (s *Store) ListIngesters(ctx context.Context) ([]config.IngesterConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	return cfg.Ingesters, nil
}

func (s *Store) PutIngester(ctx context.Context, ing config.IngesterConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, existing := range cfg.Ingesters {
		if existing.ID == ing.ID {
			cfg.Ingesters[i] = ing
			return s.flush(cfg)
		}
	}
	cfg.Ingesters = append(cfg.Ingesters, ing)
	return s.flush(cfg)
}

func (s *Store) DeleteIngester(ctx context.Context, id string) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, ing := range cfg.Ingesters {
		if ing.ID == id {
			cfg.Ingesters = append(cfg.Ingesters[:i], cfg.Ingesters[i+1:]...)
			break
		}
	}
	return s.flush(cfg)
}

// Settings

func (s *Store) GetSetting(ctx context.Context, key string) (*string, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	v, ok := cfg.Settings[key]
	if !ok {
		return nil, nil
	}
	return &v, nil
}

func (s *Store) PutSetting(ctx context.Context, key string, value string) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	cfg.Settings[key] = value
	return s.flush(cfg)
}

func (s *Store) DeleteSetting(ctx context.Context, key string) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	delete(cfg.Settings, key)
	return s.flush(cfg)
}

// Users
//
// Users are operational data (not part of the Config struct), so they are
// stored in a separate JSON file alongside the main config file.

func (s *Store) usersPath() string {
	return s.path + ".users.json"
}

func (s *Store) loadUsers() (map[string]config.User, error) {
	data, err := os.ReadFile(s.usersPath())
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]config.User), nil
		}
		return nil, fmt.Errorf("read users file: %w", err)
	}
	var users map[string]config.User
	if err := json.Unmarshal(data, &users); err != nil {
		return nil, fmt.Errorf("parse users file: %w", err)
	}
	if users == nil {
		users = make(map[string]config.User)
	}
	return users, nil
}

func (s *Store) flushUsers(users map[string]config.User) error {
	dir := filepath.Dir(s.usersPath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create users directory: %w", err)
	}
	data, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal users: %w", err)
	}
	tmpPath := s.usersPath() + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("write temp users file: %w", err)
	}
	if err := os.Rename(tmpPath, s.usersPath()); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename users file: %w", err)
	}
	return nil
}

func (s *Store) CreateUser(ctx context.Context, user config.User) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	if _, ok := users[user.Username]; ok {
		return fmt.Errorf("user %q already exists", user.Username)
	}
	users[user.Username] = user
	return s.flushUsers(users)
}

func (s *Store) GetUser(ctx context.Context, username string) (*config.User, error) {
	users, err := s.loadUsers()
	if err != nil {
		return nil, err
	}
	u, ok := users[username]
	if !ok {
		return nil, nil
	}
	return &u, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]config.User, error) {
	users, err := s.loadUsers()
	if err != nil {
		return nil, err
	}
	result := make([]config.User, 0, len(users))
	for _, u := range users {
		result = append(result, u)
	}
	return result, nil
}

func (s *Store) UpdatePassword(ctx context.Context, username string, passwordHash string) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	u, ok := users[username]
	if !ok {
		return fmt.Errorf("user %q not found", username)
	}
	u.PasswordHash = passwordHash
	u.UpdatedAt = time.Now().UTC()
	users[username] = u
	return s.flushUsers(users)
}

func (s *Store) UpdateUserRole(ctx context.Context, username string, role string) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	u, ok := users[username]
	if !ok {
		return fmt.Errorf("user %q not found", username)
	}
	u.Role = role
	u.UpdatedAt = time.Now().UTC()
	users[username] = u
	return s.flushUsers(users)
}

func (s *Store) DeleteUser(ctx context.Context, username string) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	if _, ok := users[username]; !ok {
		return fmt.Errorf("user %q not found", username)
	}
	delete(users, username)
	return s.flushUsers(users)
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	users, err := s.loadUsers()
	if err != nil {
		return 0, err
	}
	return len(users), nil
}
