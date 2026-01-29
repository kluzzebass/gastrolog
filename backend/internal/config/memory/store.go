// Package memory provides an in-memory ConfigStore implementation.
package memory

import (
	"context"
	"sync"

	"gastrolog/internal/config"
)

// Store is an in-memory ConfigStore implementation.
// Intended for testing. Configuration is not persisted across restarts.
type Store struct {
	mu  sync.RWMutex
	cfg *config.Config
}

// NewStore creates a new in-memory ConfigStore.
func NewStore() *Store {
	return &Store{}
}

// Load returns the stored configuration.
// Returns nil if no configuration has been saved.
func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.cfg == nil {
		return nil, nil
	}

	return copyConfig(s.cfg), nil
}

// Save stores the configuration in memory.
func (s *Store) Save(ctx context.Context, cfg *config.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cfg = copyConfig(cfg)
	return nil
}

// copyConfig creates a deep copy of a Config.
func copyConfig(cfg *config.Config) *config.Config {
	if cfg == nil {
		return nil
	}

	c := &config.Config{
		Receivers: make([]config.ReceiverConfig, len(cfg.Receivers)),
		Stores:    make([]config.StoreConfig, len(cfg.Stores)),
		Routes:    make([]config.RouteConfig, len(cfg.Routes)),
	}

	for i, r := range cfg.Receivers {
		c.Receivers[i] = config.ReceiverConfig{
			ID:     r.ID,
			Type:   r.Type,
			Params: copyParams(r.Params),
		}
	}

	for i, st := range cfg.Stores {
		c.Stores[i] = config.StoreConfig{
			ID:     st.ID,
			Type:   st.Type,
			Params: copyParams(st.Params),
		}
	}

	copy(c.Routes, cfg.Routes)

	return c
}

// copyParams creates a copy of a params map.
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
