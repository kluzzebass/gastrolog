// Package memory provides an in-memory ConfigStore implementation.
// Intended for testing. Configuration is not persisted across restarts.
package memory

import (
	"context"
	"sync"

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
	}
}

// Load returns the full configuration.
// Returns nil if no entities exist.
func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.filters) == 0 && len(s.rotationPolicies) == 0 && len(s.retentionPolicies) == 0 && len(s.stores) == 0 && len(s.ingesters) == 0 {
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
