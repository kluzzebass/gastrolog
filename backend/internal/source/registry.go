package source

import (
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/logging"
)

// Registry manages source identity and metadata.
// It provides fast in-memory resolution and attribute-based search.
//
// Concurrency model:
//   - Resolve/Get/Query are fully in-memory and fast
//   - New or updated sources are queued for async persistence
//   - Persistence failures do not break ingestion
//   - On startup, registry loads existing sources from store (best effort)
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Registry owns its scoped logger (component="source-registry")
//   - Logging is intentionally sparse; only lifecycle events are logged
type Registry struct {
	mu sync.RWMutex

	// Primary lookup: key → source
	byKey map[sourceKey]*Source

	// Secondary lookup: id → source
	byID map[chunk.SourceID]*Source

	// Persistence
	store     Store
	persistCh chan *Source
	stopCh    chan struct{}
	stopOnce  sync.Once
	persistWg sync.WaitGroup

	// Clock for testing
	now func() time.Time

	// Logger for this registry instance.
	// Scoped with component="source-registry" at construction time.
	logger *slog.Logger
}

// Config configures a Registry.
type Config struct {
	// Store for persistence. If nil, sources are not persisted.
	Store Store

	// PersistQueueSize is the buffer size for the async persist queue.
	// Defaults to 1000 if not set.
	PersistQueueSize int

	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time

	// Logger for structured logging. If nil, logging is disabled.
	// The registry scopes this logger with component="source-registry".
	Logger *slog.Logger
}

// NewRegistry creates a Registry with the given configuration.
// If a store is provided, existing sources are loaded at startup.
func NewRegistry(cfg Config) (*Registry, error) {
	if cfg.PersistQueueSize <= 0 {
		cfg.PersistQueueSize = 1000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "source-registry")

	r := &Registry{
		byKey:     make(map[sourceKey]*Source),
		byID:      make(map[chunk.SourceID]*Source),
		store:     cfg.Store,
		persistCh: make(chan *Source, cfg.PersistQueueSize),
		stopCh:    make(chan struct{}),
		now:       cfg.Now,
		logger:    logger,
	}

	// Load existing sources from store.
	if cfg.Store != nil {
		sources, err := cfg.Store.LoadAll()
		if err != nil {
			// Best effort: log would go here, continue without persisted data.
			// For now, we ignore the error and start fresh.
			_ = err
		} else {
			for _, src := range sources {
				key := makeKey(src.Attributes)
				r.byKey[key] = src
				r.byID[src.ID] = src
			}
		}

		// Start persistence goroutine.
		r.persistWg.Go(r.persistLoop)
	}

	return r, nil
}

// Resolve returns the SourceID for the given attributes.
// If no source exists for these attributes, a new one is created.
// This method is fast and fully in-memory.
func (r *Registry) Resolve(attrs map[string]string) chunk.SourceID {
	key := makeKey(attrs)

	// Fast path: read lock.
	r.mu.RLock()
	if src, ok := r.byKey[key]; ok {
		r.mu.RUnlock()
		return src.ID
	}
	r.mu.RUnlock()

	// Slow path: write lock.
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock.
	if src, ok := r.byKey[key]; ok {
		return src.ID
	}

	// Create new source.
	src := &Source{
		ID:         chunk.NewSourceID(),
		Attributes: copyAttrs(attrs),
		CreatedAt:  r.now(),
	}

	r.byKey[key] = src
	r.byID[src.ID] = src
	r.queuePersist(src)

	return src.ID
}

// Get retrieves a source by ID.
func (r *Registry) Get(id chunk.SourceID) (*Source, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src, ok := r.byID[id]
	if !ok {
		return nil, false
	}
	// Return a copy to prevent mutation.
	return copySource(src), true
}

// Query returns all SourceIDs whose attributes match all the given filters.
// A source matches if it contains all filter key-value pairs (subset match).
func (r *Registry) Query(filters map[string]string) []chunk.SourceID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []chunk.SourceID
	for _, src := range r.byID {
		if matchesFilters(src.Attributes, filters) {
			results = append(results, src.ID)
		}
	}
	return results
}

// Count returns the number of sources in the registry.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byID)
}

// Close stops the persistence goroutine and waits for it to finish.
// Any pending persistence operations are drained before Close returns.
func (r *Registry) Close() error {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	r.persistWg.Wait()
	return nil
}

// queuePersist sends a source to the persistence queue.
// Non-blocking: if the queue is full, the persist is dropped.
func (r *Registry) queuePersist(src *Source) {
	if r.store == nil {
		return
	}
	// Non-blocking send.
	select {
	case r.persistCh <- copySource(src):
	default:
		// Queue full, drop this persist.
		// This is acceptable: persistence is eventual and best-effort.
	}
}

// persistLoop processes the persistence queue.
func (r *Registry) persistLoop() {
	for {
		select {
		case <-r.stopCh:
			// Drain remaining items.
			for {
				select {
				case src := <-r.persistCh:
					_ = r.store.Save(src)
				default:
					return
				}
			}
		case src := <-r.persistCh:
			// Best effort: ignore errors.
			_ = r.store.Save(src)
		}
	}
}

// matchesFilters returns true if attrs contains all filter key-value pairs.
func matchesFilters(attrs, filters map[string]string) bool {
	for k, v := range filters {
		if attrs[k] != v {
			return false
		}
	}
	return true
}

// copyAttrs creates a copy of the attributes map.
func copyAttrs(attrs map[string]string) map[string]string {
	if attrs == nil {
		return nil
	}
	cp := make(map[string]string, len(attrs))
	for k, v := range attrs {
		cp[k] = v
	}
	return cp
}

// copySource creates a copy of a Source.
func copySource(src *Source) *Source {
	return &Source{
		ID:         src.ID,
		Attributes: copyAttrs(src.Attributes),
		CreatedAt:  src.CreatedAt,
	}
}
