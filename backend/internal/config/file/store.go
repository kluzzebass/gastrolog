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

	"github.com/google/uuid"
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
	path      string
	usersPath string
}

var _ config.Store = (*Store)(nil)

// NewStore creates a new file-based ConfigStore.
// configPath is the path to the config JSON file.
// usersPath is the path to the users JSON file.
func NewStore(configPath, usersPath string) *Store {
	return &Store{path: configPath, usersPath: usersPath}
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
		cfg.Filters = []config.FilterConfig{}
	}
	if cfg.RotationPolicies == nil {
		cfg.RotationPolicies = []config.RotationPolicyConfig{}
	}
	if cfg.RetentionPolicies == nil {
		cfg.RetentionPolicies = []config.RetentionPolicyConfig{}
	}
	if cfg.Settings == nil {
		cfg.Settings = make(map[string]string)
	}
	if cfg.Certs == nil {
		cfg.Certs = []config.CertPEM{}
	}
	return cfg, nil
}

// Filters

func (s *Store) GetFilter(ctx context.Context, id uuid.UUID) (*config.FilterConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	for _, fc := range cfg.Filters {
		if fc.ID == id {
			return &fc, nil
		}
	}
	return nil, nil
}

func (s *Store) ListFilters(ctx context.Context) ([]config.FilterConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	return cfg.Filters, nil
}

func (s *Store) PutFilter(ctx context.Context, fc config.FilterConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, existing := range cfg.Filters {
		if existing.ID == fc.ID {
			cfg.Filters[i] = fc
			return s.flush(cfg)
		}
	}
	cfg.Filters = append(cfg.Filters, fc)
	return s.flush(cfg)
}

func (s *Store) DeleteFilter(ctx context.Context, id uuid.UUID) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, fc := range cfg.Filters {
		if fc.ID == id {
			cfg.Filters = append(cfg.Filters[:i], cfg.Filters[i+1:]...)
			break
		}
	}
	return s.flush(cfg)
}

// Rotation policies

func (s *Store) GetRotationPolicy(ctx context.Context, id uuid.UUID) (*config.RotationPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	for _, rp := range cfg.RotationPolicies {
		if rp.ID == id {
			return &rp, nil
		}
	}
	return nil, nil
}

func (s *Store) ListRotationPolicies(ctx context.Context) ([]config.RotationPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	return cfg.RotationPolicies, nil
}

func (s *Store) PutRotationPolicy(ctx context.Context, rp config.RotationPolicyConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, existing := range cfg.RotationPolicies {
		if existing.ID == rp.ID {
			cfg.RotationPolicies[i] = rp
			return s.flush(cfg)
		}
	}
	cfg.RotationPolicies = append(cfg.RotationPolicies, rp)
	return s.flush(cfg)
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id uuid.UUID) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, rp := range cfg.RotationPolicies {
		if rp.ID == id {
			cfg.RotationPolicies = append(cfg.RotationPolicies[:i], cfg.RotationPolicies[i+1:]...)
			break
		}
	}
	return s.flush(cfg)
}

// Retention policies

func (s *Store) GetRetentionPolicy(ctx context.Context, id uuid.UUID) (*config.RetentionPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	for _, rp := range cfg.RetentionPolicies {
		if rp.ID == id {
			return &rp, nil
		}
	}
	return nil, nil
}

func (s *Store) ListRetentionPolicies(ctx context.Context) ([]config.RetentionPolicyConfig, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	return cfg.RetentionPolicies, nil
}

func (s *Store) PutRetentionPolicy(ctx context.Context, rp config.RetentionPolicyConfig) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, existing := range cfg.RetentionPolicies {
		if existing.ID == rp.ID {
			cfg.RetentionPolicies[i] = rp
			return s.flush(cfg)
		}
	}
	cfg.RetentionPolicies = append(cfg.RetentionPolicies, rp)
	return s.flush(cfg)
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id uuid.UUID) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, rp := range cfg.RetentionPolicies {
		if rp.ID == id {
			cfg.RetentionPolicies = append(cfg.RetentionPolicies[:i], cfg.RetentionPolicies[i+1:]...)
			break
		}
	}
	return s.flush(cfg)
}

// Stores

func (s *Store) GetStore(ctx context.Context, id uuid.UUID) (*config.StoreConfig, error) {
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

func (s *Store) DeleteStore(ctx context.Context, id uuid.UUID) error {
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

func (s *Store) GetIngester(ctx context.Context, id uuid.UUID) (*config.IngesterConfig, error) {
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

func (s *Store) DeleteIngester(ctx context.Context, id uuid.UUID) error {
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

// Certificates

func (s *Store) ListCertificates(ctx context.Context) ([]config.CertPEM, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil || cfg.Certs == nil {
		return nil, nil
	}
	return cfg.Certs, nil
}

func (s *Store) GetCertificate(ctx context.Context, id uuid.UUID) (*config.CertPEM, error) {
	cfg, err := s.load()
	if err != nil {
		return nil, err
	}
	if cfg == nil || cfg.Certs == nil {
		return nil, nil
	}
	for _, cert := range cfg.Certs {
		if cert.ID == id {
			return &cert, nil
		}
	}
	return nil, nil
}

func (s *Store) PutCertificate(ctx context.Context, cert config.CertPEM) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, existing := range cfg.Certs {
		if existing.ID == cert.ID {
			cfg.Certs[i] = cert
			return s.flush(cfg)
		}
	}
	cfg.Certs = append(cfg.Certs, cert)
	return s.flush(cfg)
}

func (s *Store) DeleteCertificate(ctx context.Context, id uuid.UUID) error {
	cfg, err := s.loadOrEmpty()
	if err != nil {
		return err
	}
	for i, cert := range cfg.Certs {
		if cert.ID == id {
			cfg.Certs = append(cfg.Certs[:i], cfg.Certs[i+1:]...)
			break
		}
	}
	return s.flush(cfg)
}

// Users
//
// Users are operational data (not part of the Config struct), so they are
// stored in a separate JSON file alongside the main config file.
// The map key is the user ID (UUID), not the username.

func (s *Store) loadUsers() (map[uuid.UUID]config.User, error) {
	data, err := os.ReadFile(s.usersPath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[uuid.UUID]config.User), nil
		}
		return nil, fmt.Errorf("read users file: %w", err)
	}
	var users map[uuid.UUID]config.User
	if err := json.Unmarshal(data, &users); err != nil {
		return nil, fmt.Errorf("parse users file: %w", err)
	}
	if users == nil {
		users = make(map[uuid.UUID]config.User)
	}
	return users, nil
}

func (s *Store) flushUsers(users map[uuid.UUID]config.User) error {
	dir := filepath.Dir(s.usersPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create users directory: %w", err)
	}
	data, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal users: %w", err)
	}
	tmpPath := s.usersPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("write temp users file: %w", err)
	}
	if err := os.Rename(tmpPath, s.usersPath); err != nil {
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
	// Check for duplicate ID.
	if _, ok := users[user.ID]; ok {
		return fmt.Errorf("user with ID %q already exists", user.ID)
	}
	// Check for duplicate username.
	for _, u := range users {
		if u.Username == user.Username {
			return fmt.Errorf("user %q already exists", user.Username)
		}
	}
	users[user.ID] = user
	return s.flushUsers(users)
}

func (s *Store) GetUser(ctx context.Context, id uuid.UUID) (*config.User, error) {
	users, err := s.loadUsers()
	if err != nil {
		return nil, err
	}
	u, ok := users[id]
	if !ok {
		return nil, nil
	}
	return &u, nil
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*config.User, error) {
	users, err := s.loadUsers()
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		if u.Username == username {
			return &u, nil
		}
	}
	return nil, nil
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

func (s *Store) UpdatePassword(ctx context.Context, id uuid.UUID, passwordHash string) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	u, ok := users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.PasswordHash = passwordHash
	u.UpdatedAt = time.Now().UTC()
	users[id] = u
	return s.flushUsers(users)
}

func (s *Store) UpdateUserRole(ctx context.Context, id uuid.UUID, role string) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	u, ok := users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.Role = role
	u.UpdatedAt = time.Now().UTC()
	users[id] = u
	return s.flushUsers(users)
}

func (s *Store) DeleteUser(ctx context.Context, id uuid.UUID) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	if _, ok := users[id]; !ok {
		return fmt.Errorf("user %q not found", id)
	}
	delete(users, id)
	return s.flushUsers(users)
}

func (s *Store) InvalidateTokens(ctx context.Context, id uuid.UUID, at time.Time) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	u, ok := users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.TokenInvalidatedAt = at
	u.UpdatedAt = time.Now().UTC()
	users[id] = u
	return s.flushUsers(users)
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	users, err := s.loadUsers()
	if err != nil {
		return 0, err
	}
	return len(users), nil
}

func (s *Store) GetUserPreferences(ctx context.Context, id uuid.UUID) (*string, error) {
	users, err := s.loadUsers()
	if err != nil {
		return nil, err
	}
	u, ok := users[id]
	if !ok {
		return nil, nil
	}
	if u.Preferences == "" {
		return nil, nil
	}
	return &u.Preferences, nil
}

func (s *Store) PutUserPreferences(ctx context.Context, id uuid.UUID, prefs string) error {
	users, err := s.loadUsers()
	if err != nil {
		return err
	}
	u, ok := users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.Preferences = prefs
	u.UpdatedAt = time.Now().UTC()
	users[id] = u
	return s.flushUsers(users)
}
