package file

import (
	"os"
	"path/filepath"
	"slices"
	"time"

	"gastrolog/internal/config"
)

const defaultCacheBudget = 1 << 30 // 1 GiB

type cacheEntry struct {
	path    string
	size    int64
	modTime time.Time
}

// EvictCache scans the cache directory and removes entries that exceed the
// configured budget or TTL. Safe to call concurrently — operates on filesystem
// only, no Manager lock required.
//
// LRU mode: evicts least-recently-modified files until total size ≤ budget.
// TTL mode: evicts files older than the configured TTL, then applies the budget cap.
func (m *Manager) EvictCache() {
	if m.cfg.CacheDir == "" {
		return
	}

	var budget uint64
	if m.cfg.CacheBudget != "" {
		parsed, err := config.ParseSize(m.cfg.CacheBudget)
		if err == nil {
			budget = parsed
		}
	}
	if budget == 0 {
		budget = defaultCacheBudget
	}

	entries, totalSize := m.scanCache()
	if len(entries) == 0 {
		return
	}

	eviction := m.cfg.CacheEviction
	if eviction == "" {
		eviction = "lru"
	}

	var evicted int
	switch eviction {
	case "ttl":
		entries, totalSize, evicted = m.evictByTTL(entries, totalSize)
		// After TTL eviction, also enforce budget cap.
		if totalSize > int64(budget) {
			_, _, budgetEvicted := m.evictBySize(entries, totalSize, int64(budget))
			evicted += budgetEvicted
		}
	default: // lru
		if totalSize <= int64(budget) {
			return
		}
		_, _, evicted = m.evictBySize(entries, totalSize, int64(budget))
	}

	if evicted > 0 {
		m.logger.Debug("cache eviction completed",
			"evicted", evicted, "policy", eviction)
	}
}

// scanCache reads the cache directory and returns entries sorted by modTime (oldest first).
func (m *Manager) scanCache() ([]cacheEntry, int64) {
	dirEntries, err := os.ReadDir(m.cfg.CacheDir)
	if err != nil {
		return nil, 0
	}

	entries := make([]cacheEntry, 0, len(dirEntries))
	var totalSize int64
	for _, de := range dirEntries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".glcb" {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		entries = append(entries, cacheEntry{
			path:    filepath.Join(m.cfg.CacheDir, de.Name()),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		totalSize += info.Size()
	}

	// Sort oldest first — used by both LRU (evict oldest) and TTL.
	slices.SortFunc(entries, func(a, b cacheEntry) int {
		return a.modTime.Compare(b.modTime)
	})

	return entries, totalSize
}

// evictByTTL removes entries older than the configured CacheTTL.
func (m *Manager) evictByTTL(entries []cacheEntry, totalSize int64) ([]cacheEntry, int64, int) {
	ttl, err := config.ParseDuration(m.cfg.CacheTTL)
	if err != nil || ttl < 0 {
		return entries, totalSize, 0
	}

	cutoff := time.Now().Add(-ttl)
	var kept []cacheEntry
	var evicted int

	for _, e := range entries {
		if e.modTime.Before(cutoff) {
			if err := os.Remove(e.path); err == nil {
				totalSize -= e.size
				evicted++
			}
		} else {
			kept = append(kept, e)
		}
	}
	return kept, totalSize, evicted
}

// evictBySize removes oldest entries until totalSize ≤ budget.
func (m *Manager) evictBySize(entries []cacheEntry, totalSize, budget int64) ([]cacheEntry, int64, int) {
	var kept []cacheEntry
	var evicted int

	for _, e := range entries {
		if totalSize <= budget {
			kept = append(kept, e)
			continue
		}
		if err := os.Remove(e.path); err == nil {
			totalSize -= e.size
			evicted++
		} else {
			kept = append(kept, e)
		}
	}
	return kept, totalSize, evicted
}
