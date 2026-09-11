package lookup

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/logging/comp"
)

// maxConcurrentResolves bounds the reverse-DNS resolutions in flight at
// once. Without it a wide result set turns into a burst at whatever
// resolver the node is configured with, multiplied by every node in a
// fan-out.
const maxConcurrentResolves = 8

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
	inFlight  chan struct{} // capacity bounds concurrent resolutions
	logger    *slog.Logger

	// lookupAddr is the resolution itself, separated so a test drives the
	// real LookupValues — budget, cap, cache and all — instead of a second
	// copy of it.
	lookupAddr func(ctx context.Context, addr string) ([]string, error)

	mu    sync.Mutex
	cache map[string]rdnsEntry
}

// NewRDNS creates a reverse DNS lookup table.
func NewRDNS(logger *slog.Logger) *RDNS {
	r := &RDNS{
		resolver:  net.DefaultResolver,
		timeout:   2 * time.Second,
		posTTL:    5 * time.Minute,
		negTTL:    1 * time.Minute,
		cacheSize: 10_000,
		inFlight:  make(chan struct{}, maxConcurrentResolves),
		logger: comp.Root("lookup").Desc(
			"Lookup tables that enrich records at query time — HTTP, file-backed, MMDB and static.",
		).Apply(logging.Default(logger)),
		cache: make(map[string]rdnsEntry),
	}
	r.lookupAddr = r.resolver.LookupAddr
	return r
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

	// A cache miss is what costs a resolution, so it is what the per-query
	// budget pays for. The budget is shared with every other lookup kind, so
	// one query has one outbound allowance however it is written.
	allowed, firstRefusal := spendOutbound(ctx)
	if !allowed {
		if firstRefusal {
			limit, _ := outboundLimit(ctx)
			r.logger.Warn("outbound lookup budget exhausted; remaining records go unenriched",
				"table", "rdns", "limit", limit)
		}
		return nil
	}

	select {
	case r.inFlight <- struct{}{}:
	case <-ctx.Done():
		return nil
	}

	// Resolve with timeout.
	lookupCtx, cancel := context.WithTimeout(ctx, r.timeout)
	names, err := r.lookupAddr(lookupCtx, value)
	cancel()
	<-r.inFlight

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
