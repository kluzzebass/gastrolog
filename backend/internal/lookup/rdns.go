package lookup

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"
)

// rdnsEntry is a cached reverse DNS result.
type rdnsEntry struct {
	hostname string
	expires  time.Time
}

// RDNS is a reverse DNS lookup table. It resolves IP addresses to hostnames
// using net.Resolver.LookupAddr with a TTL cache.
type RDNS struct {
	resolver  *net.Resolver
	timeout   time.Duration
	posTTL    time.Duration // positive result TTL
	negTTL    time.Duration // negative (miss) result TTL
	cacheSize int

	mu    sync.Mutex
	cache map[string]rdnsEntry
}

// NewRDNS creates a reverse DNS lookup table.
func NewRDNS() *RDNS {
	return &RDNS{
		resolver:  net.DefaultResolver,
		timeout:   2 * time.Second,
		posTTL:    5 * time.Minute,
		negTTL:    1 * time.Minute,
		cacheSize: 10_000,
		cache:     make(map[string]rdnsEntry),
	}
}

// Parameters returns the single input parameter name.
func (r *RDNS) Parameters() []string { return []string{"value"} }

// Suffixes returns the output suffixes for RDNS lookups.
func (r *RDNS) Suffixes() []string {
	return []string{"hostname"}
}

// LookupValues resolves an IP address to a hostname. Returns nil on failure.
func (r *RDNS) LookupValues(ctx context.Context, values map[string]string) map[string]string {
	value := values["value"]
	if value == "" {
		return nil
	}

	// Check cache.
	r.mu.Lock()
	if entry, ok := r.cache[value]; ok {
		if time.Now().Before(entry.expires) {
			r.mu.Unlock()
			if entry.hostname == "" {
				return nil // cached negative
			}
			return map[string]string{"hostname": entry.hostname}
		}
		// Expired — will re-resolve below.
	}
	r.mu.Unlock()

	// Resolve with timeout.
	lookupCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	names, err := r.resolver.LookupAddr(lookupCtx, value)

	var hostname string
	if err == nil && len(names) > 0 {
		hostname = strings.TrimSuffix(names[0], ".")
	}

	// Cache the result.
	ttl := r.negTTL
	if hostname != "" {
		ttl = r.posTTL
	}
	r.mu.Lock()
	if len(r.cache) >= r.cacheSize {
		clear(r.cache)
	}
	r.cache[value] = rdnsEntry{hostname: hostname, expires: time.Now().Add(ttl)}
	r.mu.Unlock()

	if hostname == "" {
		return nil
	}
	return map[string]string{"hostname": hostname}
}
