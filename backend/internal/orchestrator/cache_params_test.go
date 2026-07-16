package orchestrator

// Coverage for gastrolog-338j51: the warm-cache config must actually reach
// the file-manager factory. Before this, nothing populated the cache_* param
// keys the factory reads, so an operator's cache-budget was silently ignored
// and the cache ran unbounded. This asserts the config → param bridge that
// buildVaultParams' memory-budget already had and the cache config lacked.

import (
	"testing"
	"time"

	"gastrolog/internal/system"
)

func TestAddCacheParamsThreadsConfigToFactory(t *testing.T) {
	t.Parallel()
	params := map[string]string{}
	addCacheParams(params, system.VaultConfig{
		CacheEviction:    "lru",
		CacheBudgetBytes: 2 << 30, // 2 GiB
		CacheTTLNanos:    int64(time.Hour),
	})

	// Keys the file-manager factory (applyCloudAndCacheParams) reads.
	if got := params["cache_eviction"]; got != "lru" {
		t.Errorf("cache_eviction = %q, want lru", got)
	}
	// Budget is threaded as a plain byte count; the factory's ParseSize
	// accepts that and yields the same value lruRule enforces.
	if got := params["cache_budget"]; got != "2147483648" {
		t.Errorf("cache_budget = %q, want 2147483648 (2 GiB)", got)
	}
	// TTL is threaded as a duration string the factory's ParseDuration reads.
	if got := params["cache_ttl"]; got != "1h0m0s" {
		t.Errorf("cache_ttl = %q, want 1h0m0s", got)
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
