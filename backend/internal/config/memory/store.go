// Package memory provides an in-memory ConfigStore implementation.
// Intended for testing. Configuration is not persisted across restarts.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gastrolog/internal/config"
)

// Store is an in-memory ConfigStore implementation.
type Store struct {
	mu                sync.RWMutex
	filters           map[string]config.FilterConfig
	rotationPolicies  map[string]config.RotationPolicyConfig
	retentionPolicies map[string]config.RetentionPolicyConfig
	stores            map[string]config.StoreConfig
	ingesters         map[string]config.IngesterConfig
	settings          map[string]string
	users             map[string]config.User
}

var _ config.Store = (*Store)(nil)

// NewStore creates a new in-memory ConfigStore.
func NewStore() *Store {
	return &Store{
		filters:           make(map[string]config.FilterConfig),
		rotationPolicies:  make(map[string]config.RotationPolicyConfig),
		retentionPolicies: make(map[string]config.RetentionPolicyConfig),
		stores:            make(map[string]config.StoreConfig),
		ingesters:         make(map[string]config.IngesterConfig),
		settings:          make(map[string]string),
		users:             make(map[string]config.User),
	}
}

// Load returns the full configuration.
// Returns nil if no entities exist.
func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.filters) == 0 && len(s.rotationPolicies) == 0 && len(s.retentionPolicies) == 0 && len(s.stores) == 0 && len(s.ingesters) == 0 && len(s.settings) == 0 {
		return nil, nil
	}

	cfg := &config.Config{}

	if len(s.filters) > 0 {
		cfg.Filters = make(map[string]config.FilterConfig, len(s.filters))
		for id, fc := range s.filters {
			cfg.Filters[id] = copyFilterConfig(fc)
		}
	}

	if len(s.rotationPolicies) > 0 {
		cfg.RotationPolicies = make(map[string]config.RotationPolicyConfig, len(s.rotationPolicies))
		for id, rp := range s.rotationPolicies {
			cfg.RotationPolicies[id] = copyRotationPolicy(rp)
		}
	}

	if len(s.retentionPolicies) > 0 {
		cfg.RetentionPolicies = make(map[string]config.RetentionPolicyConfig, len(s.retentionPolicies))
		for id, rp := range s.retentionPolicies {
			cfg.RetentionPolicies[id] = copyRetentionPolicy(rp)
		}
	}

	if len(s.stores) > 0 {
		cfg.Stores = make([]config.StoreConfig, 0, len(s.stores))
		for _, st := range s.stores {
			cfg.Stores = append(cfg.Stores, copyStoreConfig(st))
		}
	}

	if len(s.ingesters) > 0 {
		cfg.Ingesters = make([]config.IngesterConfig, 0, len(s.ingesters))
		for _, ing := range s.ingesters {
			cfg.Ingesters = append(cfg.Ingesters, copyIngesterConfig(ing))
		}
	}

	if len(s.settings) > 0 {
		cfg.Settings = make(map[string]string, len(s.settings))
		for k, v := range s.settings {
			cfg.Settings[k] = v
		}
	}

	return cfg, nil
}

// Filters

func (s *Store) GetFilter(ctx context.Context, id string) (*config.FilterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fc, ok := s.filters[id]
	if !ok {
		return nil, nil
	}
	c := copyFilterConfig(fc)
	return &c, nil
}

func (s *Store) ListFilters(ctx context.Context) (map[string]config.FilterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]config.FilterConfig, len(s.filters))
	for id, fc := range s.filters {
		result[id] = copyFilterConfig(fc)
	}
	return result, nil
}

func (s *Store) PutFilter(ctx context.Context, id string, cfg config.FilterConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.filters[id] = copyFilterConfig(cfg)
	return nil
}

func (s *Store) DeleteFilter(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.filters, id)
	return nil
}

// Rotation policies

func (s *Store) GetRotationPolicy(ctx context.Context, id string) (*config.RotationPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rp, ok := s.rotationPolicies[id]
	if !ok {
		return nil, nil
	}
	c := copyRotationPolicy(rp)
	return &c, nil
}

func (s *Store) ListRotationPolicies(ctx context.Context) (map[string]config.RotationPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]config.RotationPolicyConfig, len(s.rotationPolicies))
	for id, rp := range s.rotationPolicies {
		result[id] = copyRotationPolicy(rp)
	}
	return result, nil
}

func (s *Store) PutRotationPolicy(ctx context.Context, id string, cfg config.RotationPolicyConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rotationPolicies[id] = copyRotationPolicy(cfg)
	return nil
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.rotationPolicies, id)
	return nil
}

// Retention policies

func (s *Store) GetRetentionPolicy(ctx context.Context, id string) (*config.RetentionPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rp, ok := s.retentionPolicies[id]
	if !ok {
		return nil, nil
	}
	c := copyRetentionPolicy(rp)
	return &c, nil
}

func (s *Store) ListRetentionPolicies(ctx context.Context) (map[string]config.RetentionPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]config.RetentionPolicyConfig, len(s.retentionPolicies))
	for id, rp := range s.retentionPolicies {
		result[id] = copyRetentionPolicy(rp)
	}
	return result, nil
}

func (s *Store) PutRetentionPolicy(ctx context.Context, id string, cfg config.RetentionPolicyConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retentionPolicies[id] = copyRetentionPolicy(cfg)
	return nil
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.retentionPolicies, id)
	return nil
}

// Stores

func (s *Store) GetStore(ctx context.Context, id string) (*config.StoreConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st, ok := s.stores[id]
	if !ok {
		return nil, nil
	}
	c := copyStoreConfig(st)
	return &c, nil
}

func (s *Store) ListStores(ctx context.Context) ([]config.StoreConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.StoreConfig, 0, len(s.stores))
	for _, st := range s.stores {
		result = append(result, copyStoreConfig(st))
	}
	return result, nil
}

func (s *Store) PutStore(ctx context.Context, cfg config.StoreConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stores[cfg.ID] = copyStoreConfig(cfg)
	return nil
}

func (s *Store) DeleteStore(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.stores, id)
	return nil
}

// Ingesters

func (s *Store) GetIngester(ctx context.Context, id string) (*config.IngesterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ing, ok := s.ingesters[id]
	if !ok {
		return nil, nil
	}
	c := copyIngesterConfig(ing)
	return &c, nil
}

func (s *Store) ListIngesters(ctx context.Context) ([]config.IngesterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.IngesterConfig, 0, len(s.ingesters))
	for _, ing := range s.ingesters {
		result = append(result, copyIngesterConfig(ing))
	}
	return result, nil
}

func (s *Store) PutIngester(ctx context.Context, cfg config.IngesterConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ingesters[cfg.ID] = copyIngesterConfig(cfg)
	return nil
}

func (s *Store) DeleteIngester(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.ingesters, id)
	return nil
}

// Settings

func (s *Store) GetSetting(ctx context.Context, key string) (*string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.settings[key]
	if !ok {
		return nil, nil
	}
	return &v, nil
}

func (s *Store) PutSetting(ctx context.Context, key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.settings[key] = value
	return nil
}

func (s *Store) DeleteSetting(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.settings, key)
	return nil
}

// Users

func (s *Store) CreateUser(ctx context.Context, user config.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[user.Username]; ok {
		return fmt.Errorf("user %q already exists", user.Username)
	}
	s.users[user.Username] = user
	return nil
}

func (s *Store) GetUser(ctx context.Context, username string) (*config.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	u, ok := s.users[username]
	if !ok {
		return nil, nil
	}
	return &u, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]config.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make([]config.User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	return users, nil
}

func (s *Store) UpdatePassword(ctx context.Context, username string, passwordHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[username]
	if !ok {
		return fmt.Errorf("user %q not found", username)
	}
	u.PasswordHash = passwordHash
	u.UpdatedAt = time.Now().UTC()
	s.users[username] = u
	return nil
}

func (s *Store) UpdateUserRole(ctx context.Context, username string, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[username]
	if !ok {
		return fmt.Errorf("user %q not found", username)
	}
	u.Role = role
	u.UpdatedAt = time.Now().UTC()
	s.users[username] = u
	return nil
}

func (s *Store) DeleteUser(ctx context.Context, username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[username]; !ok {
		return fmt.Errorf("user %q not found", username)
	}
	delete(s.users, username)
	return nil
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.users), nil
}

// Deep copy helpers

func copyFilterConfig(fc config.FilterConfig) config.FilterConfig {
	return config.FilterConfig{Expression: fc.Expression}
}

func copyRotationPolicy(rp config.RotationPolicyConfig) config.RotationPolicyConfig {
	c := config.RotationPolicyConfig{}
	if rp.MaxBytes != nil {
		c.MaxBytes = config.StringPtr(*rp.MaxBytes)
	}
	if rp.MaxAge != nil {
		c.MaxAge = config.StringPtr(*rp.MaxAge)
	}
	if rp.MaxRecords != nil {
		c.MaxRecords = config.Int64Ptr(*rp.MaxRecords)
	}
	if rp.Cron != nil {
		c.Cron = config.StringPtr(*rp.Cron)
	}
	return c
}

func copyRetentionPolicy(rp config.RetentionPolicyConfig) config.RetentionPolicyConfig {
	c := config.RetentionPolicyConfig{}
	if rp.MaxAge != nil {
		c.MaxAge = config.StringPtr(*rp.MaxAge)
	}
	if rp.MaxBytes != nil {
		c.MaxBytes = config.StringPtr(*rp.MaxBytes)
	}
	if rp.MaxChunks != nil {
		c.MaxChunks = config.Int64Ptr(*rp.MaxChunks)
	}
	return c
}

func copyStoreConfig(st config.StoreConfig) config.StoreConfig {
	c := config.StoreConfig{
		ID:     st.ID,
		Type:   st.Type,
		Params: copyParams(st.Params),
	}
	if st.Filter != nil {
		c.Filter = config.StringPtr(*st.Filter)
	}
	if st.Policy != nil {
		c.Policy = config.StringPtr(*st.Policy)
	}
	if st.Retention != nil {
		c.Retention = config.StringPtr(*st.Retention)
	}
	return c
}

func copyIngesterConfig(ing config.IngesterConfig) config.IngesterConfig {
	return config.IngesterConfig{
		ID:     ing.ID,
		Type:   ing.Type,
		Params: copyParams(ing.Params),
	}
}

func copyParams(params map[string]string) map[string]string {
	if params == nil {
		return nil
	}
	cp := make(map[string]string, len(params))
	for k, v := range params {
		cp[k] = v
	}
	return cp
}
