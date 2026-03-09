package lookup

import (
	"context"
	"net"
	"testing"
	"time"
)

// testResolver creates a net.Resolver that uses a custom dial function
// to intercept DNS lookups. For reverse DNS, the Go stdlib dials the
// system resolver, so we override DialContext to simulate responses.
// Instead, we test through the RDNS type directly with a thin wrapper.

// mockRDNS wraps RDNS but overrides the lookup to use a map.
type mockRDNS struct {
	*RDNS
	responses map[string]string // ip → hostname
}

func newMockRDNS(responses map[string]string) *mockRDNS {
	// Use a resolver that will fail (we override Lookup).
	r := NewRDNS()
	m := &mockRDNS{RDNS: r, responses: responses}
	return m
}

func (m *mockRDNS) LookupValues(ctx context.Context, values map[string]string) map[string]string {
	value := values["value"]
	if value == "" {
		return nil
	}

	// Check cache first.
	m.mu.Lock()
	if entry, ok := m.cache[value]; ok {
		if time.Now().Before(entry.expires) {
			m.mu.Unlock()
			if entry.hostname == "" {
				return nil
			}
			return map[string]string{"hostname": entry.hostname}
		}
	}
	m.mu.Unlock()

	// Mock resolve.
	hostname := m.responses[value]

	// Cache the result.
	ttl := m.negTTL
	if hostname != "" {
		ttl = m.posTTL
	}
	m.mu.Lock()
	if len(m.cache) >= m.cacheSize {
		clear(m.cache)
	}
	m.cache[value] = rdnsEntry{hostname: hostname, expires: time.Now().Add(ttl)}
	m.mu.Unlock()

	if hostname == "" {
		return nil
	}
	return map[string]string{"hostname": hostname}
}

func TestRDNSSuffixes(t *testing.T) {
	t.Parallel()
	r := NewRDNS()
	suffixes := r.Suffixes()
	if len(suffixes) != 1 || suffixes[0] != "hostname" {
		t.Errorf("Suffixes() = %v, want [hostname]", suffixes)
	}
}

func TestRDNSLookupHit(t *testing.T) {
	t.Parallel()
	m := newMockRDNS(map[string]string{
		"8.8.8.8": "dns.google",
	})

	result := m.LookupValues(context.Background(), map[string]string{"value": "8.8.8.8"})
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result["hostname"] != "dns.google" {
		t.Errorf("hostname = %q, want 'dns.google'", result["hostname"])
	}
}

func TestRDNSLookupMiss(t *testing.T) {
	t.Parallel()
	m := newMockRDNS(map[string]string{})

	result := m.LookupValues(context.Background(), map[string]string{"value": "192.168.1.1"})
	if result != nil {
		t.Errorf("expected nil for unknown IP, got %v", result)
	}
}

func TestRDNSLookupEmpty(t *testing.T) {
	t.Parallel()
	m := newMockRDNS(map[string]string{})

	result := m.LookupValues(context.Background(), map[string]string{"value": ""})
	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

func TestRDNSCacheHit(t *testing.T) {
	t.Parallel()
	callCount := 0
	responses := map[string]string{"1.2.3.4": "host.example.com"}
	m := newMockRDNS(responses)

	// Override to track calls.
	origResponses := m.responses
	m.responses = map[string]string{}
	// Warm the cache.
	m.responses = origResponses
	_ = m.LookupValues(context.Background(), map[string]string{"value": "1.2.3.4"})

	// Clear responses so a new resolve would return nothing.
	m.responses = map[string]string{}
	callCount++

	// Second call should use cache.
	result := m.LookupValues(context.Background(), map[string]string{"value": "1.2.3.4"})
	if result == nil || result["hostname"] != "host.example.com" {
		t.Errorf("cached lookup failed: got %v", result)
	}
}

func TestRDNSCacheNegative(t *testing.T) {
	t.Parallel()
	m := newMockRDNS(map[string]string{})

	// First call caches negative.
	_ = m.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})

	// Add a response — but cache should still return nil.
	m.responses["10.0.0.1"] = "now-resolvable.example.com"

	result := m.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
	if result != nil {
		t.Errorf("expected cached negative, got %v", result)
	}
}

func TestRDNSLookupTableInterface(t *testing.T) {
	t.Parallel()
	// Verify RDNS implements LookupTable.
	var _ LookupTable = (*RDNS)(nil)
}

func TestRegistryResolve(t *testing.T) {
	t.Parallel()
	r := Registry{
		"rdns": NewRDNS(),
	}

	if r.Resolve("rdns") == nil {
		t.Error("expected rdns table, got nil")
	}
	if r.Resolve("unknown") != nil {
		t.Error("expected nil for unknown table")
	}
}

func TestNewRDNSDefaults(t *testing.T) {
	t.Parallel()
	r := NewRDNS()
	if r.resolver != net.DefaultResolver {
		t.Error("default resolver should be net.DefaultResolver")
	}
	if r.timeout != 2*time.Second {
		t.Errorf("default timeout = %v, want 2s", r.timeout)
	}
	if r.posTTL != 5*time.Minute {
		t.Errorf("default posTTL = %v, want 5m", r.posTTL)
	}
	if r.negTTL != 1*time.Minute {
		t.Errorf("default negTTL = %v, want 1m", r.negTTL)
	}
	if r.cacheSize != 10_000 {
		t.Errorf("default cacheSize = %d, want 10000", r.cacheSize)
	}
}
