package orchestrator

// Coverage for gastrolog-338j51: the warm-cache config must actually reach
// the file-manager factory. Before this, nothing populated the cache_* param
// keys the factory reads, so an operator's cache-budget was silently ignored
// and the cache ran unbounded. This asserts the config → param bridge that
// buildVaultParams' memory-budget already had and the cache config lacked.

import (
	"testing"

	"gastrolog/internal/system"
)

func TestAddCacheParamsThreadsConfigToFactory(t *testing.T) {
	t.Parallel()
	params := map[string]string{}
	addCacheParams(params, system.VaultConfig{
		CacheEviction: "lru",
		CacheBudget:   "2GiB",
		CacheTTL:      "1h",
	})

	// Expressions pass through verbatim — the factory resolves them with the
	// shared parser (gastrolog-etcjdx).
	if got := params["cache_eviction"]; got != "lru" {
		t.Errorf("cache_eviction = %q, want lru", got)
	}
	if got := params["cache_budget"]; got != "2GiB" {
		t.Errorf("cache_budget = %q, want 2GiB", got)
	}
	if got := params["cache_ttl"]; got != "1h" {
		t.Errorf("cache_ttl = %q, want 1h", got)
	}
}

// Zero-valued cache fields are omitted, so a cloud vault with no cache config
// does not force empty keys (the factory treats missing and empty alike).
func TestAddCacheParamsOmitsUnsetFields(t *testing.T) {
	t.Parallel()
	params := map[string]string{}
	addCacheParams(params, system.VaultConfig{})
	for _, k := range []string{"cache_eviction", "cache_budget", "cache_ttl"} {
		if _, ok := params[k]; ok {
			t.Errorf("unset config wrote %q", k)
		}
	}
}
