package lookup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// rdnsWithHosts builds a real RDNS whose resolution answers from a map, so
// the tests drive the actual LookupValues — cache, outbound budget and
// concurrency cap included — rather than a second copy of it.
func rdnsWithHosts(hosts map[string]string) (*RDNS, *resolveCounter) {
	r := NewRDNS(nil)
	counter := &resolveCounter{hosts: hosts}
	r.lookupAddr = counter.lookup
	return r, counter
}

// resolveCounter answers reverse lookups from a map and records how many
// resolutions actually left the cache, plus the high-water mark of
// concurrent ones.
type resolveCounter struct {
	mu       sync.Mutex
	hosts    map[string]string
	calls    int
	inFlight int
	peak     int
	block    chan struct{} // when non-nil,every resolution waits on it
}

func (c *resolveCounter) lookup(_ context.Context, addr string) ([]string, error) {
	c.mu.Lock()
	c.calls++
	c.inFlight++
	if c.inFlight > c.peak {
		c.peak = c.inFlight
	}
	host, ok := c.hosts[addr]
	block := c.block
	c.mu.Unlock()

	if block != nil {
		<-block
	}

	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()

	if !ok {
		return nil, errors.New("no such host")
	}
	return []string{host + "."}, nil
}

func (c *resolveCounter) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *resolveCounter) peakInFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peak
}

func (c *resolveCounter) setHost(addr, host string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hosts[addr] = host
}

func TestRDNSSuffixes(t *testing.T) {
	t.Parallel()
	r := NewRDNS(nil)
	suffixes := r.Suffixes()
	if len(suffixes) != 1 || suffixes[0] != "hostname" {
		t.Errorf("Suffixes() = %v, want [hostname]", suffixes)
	}
}

func TestRDNSLookupHit(t *testing.T) {
	t.Parallel()
	r, _ := rdnsWithHosts(map[string]string{"8.8.8.8": "dns.google"})

	result := r.LookupValues(context.Background(), map[string]string{"value": "8.8.8.8"})
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result["hostname"] != "dns.google" {
		t.Errorf("hostname = %q, want 'dns.google'", result["hostname"])
	}
}

func TestRDNSLookupMiss(t *testing.T) {
	t.Parallel()
	r, _ := rdnsWithHosts(map[string]string{})

	result := r.LookupValues(context.Background(), map[string]string{"value": "192.168.1.1"})
	if result != nil {
		t.Errorf("expected nil for unknown IP, got %v", result)
	}
}

func TestRDNSLookupEmpty(t *testing.T) {
	t.Parallel()
	r, _ := rdnsWithHosts(map[string]string{})

	result := r.LookupValues(context.Background(), map[string]string{"value": ""})
	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

func TestRDNSCacheHit(t *testing.T) {
	t.Parallel()
	r, counter := rdnsWithHosts(map[string]string{"1.2.3.4": "host.example.com"})
	ctx := context.Background()

	_ = r.LookupValues(ctx, map[string]string{"value": "1.2.3.4"})
	if counter.callCount() != 1 {
		t.Fatalf("warming the cache took %d resolutions, want 1", counter.callCount())
	}

	result := r.LookupValues(ctx, map[string]string{"value": "1.2.3.4"})
	if result == nil || result["hostname"] != "host.example.com" {
		t.Errorf("cached lookup failed: got %v", result)
	}
	if counter.callCount() != 1 {
		t.Errorf("the second lookup resolved again: %d resolutions total", counter.callCount())
	}
}

func TestRDNSCacheNegative(t *testing.T) {
	t.Parallel()
	r, counter := rdnsWithHosts(map[string]string{})

	// First call caches negative.
	_ = r.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})

	// Add a response — but cache should still return nil.
	counter.setHost("10.0.0.1", "now-resolvable.example.com")

	result := r.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
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
		"rdns": NewRDNS(nil),
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
	r := NewRDNS(nil)
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

// Reverse DNS is an outbound request per distinct value, on every node in a
// fan-out. It spends the same per-query allowance the HTTP lookup does, so
// a wide result set cannot turn one query into unbounded DNS traffic.
func TestRDNSSpendsTheOutboundBudget(t *testing.T) {
	t.Parallel()
	const budget = 3
	hosts := map[string]string{}
	for i := range 10 {
		hosts[fmt.Sprintf("10.0.0.%d", i)] = fmt.Sprintf("host-%d.example.com", i)
	}
	r, counter := rdnsWithHosts(hosts)
	ctx := WithOutboundBudget(context.Background(), budget)

	enriched := 0
	for i := range 10 {
		if r.LookupValues(ctx, map[string]string{"value": fmt.Sprintf("10.0.0.%d", i)}) != nil {
			enriched++
		}
	}

	if counter.callCount() != budget {
		t.Errorf("resolved %d addresses on a budget of %d", counter.callCount(), budget)
	}
	if enriched != budget {
		t.Errorf("enriched %d records, want %d — the rest go unenriched, not unbounded", enriched, budget)
	}
}

// The allowance is one per query across every lookup kind, so a query
// cannot get a second budget by switching tables.
func TestRDNSSharesTheBudgetWithOtherLookups(t *testing.T) {
	t.Parallel()
	r, counter := rdnsWithHosts(map[string]string{"10.0.0.1": "host.example.com"})
	ctx := WithOutboundBudget(context.Background(), 1)

	// Something else already spent the query's only allowance.
	if allowed, _ := spendOutbound(ctx); !allowed {
		t.Fatal("premise: the first spend must be allowed")
	}

	if got := r.LookupValues(ctx, map[string]string{"value": "10.0.0.1"}); got != nil {
		t.Errorf("rdns enriched past an exhausted budget: %v", got)
	}
	if counter.callCount() != 0 {
		t.Errorf("rdns resolved %d addresses past an exhausted budget", counter.callCount())
	}
}

// A cached value costs nothing, so repeats of one address do not eat the
// allowance the rest of the result set needs.
func TestRDNSCacheHitCostsNoBudget(t *testing.T) {
	t.Parallel()
	r, counter := rdnsWithHosts(map[string]string{"10.0.0.1": "host.example.com"})
	ctx := WithOutboundBudget(context.Background(), 2)

	for range 5 {
		if r.LookupValues(ctx, map[string]string{"value": "10.0.0.1"}) == nil {
			t.Fatal("a cached address stopped resolving; the cache is not serving repeats")
		}
	}
	if counter.callCount() != 1 {
		t.Errorf("%d resolutions for one repeated address, want 1", counter.callCount())
	}
}

// Concurrent queries must not multiply into a burst at the node's resolver.
func TestRDNSCapsConcurrentResolutions(t *testing.T) {
	t.Parallel()
	hosts := map[string]string{}
	const addrs = maxConcurrentResolves * 4
	for i := range addrs {
		hosts[fmt.Sprintf("10.1.0.%d", i)] = fmt.Sprintf("host-%d.example.com", i)
	}
	r, counter := rdnsWithHosts(hosts)

	// Every resolution parks until released, so the in-flight count is
	// whatever the cap allows rather than whatever the scheduler managed.
	release := make(chan struct{})
	counter.mu.Lock()
	counter.block = release
	counter.mu.Unlock()

	ctx := WithOutboundBudget(context.Background(), addrs)
	var wg sync.WaitGroup
	for i := range addrs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.LookupValues(ctx, map[string]string{"value": fmt.Sprintf("10.1.0.%d", i)})
		}()
	}

	// Let them pile up against the cap, then drain.
	waitForInFlight(t, counter, maxConcurrentResolves)
	close(release)
	wg.Wait()

	if peak := counter.peakInFlight(); peak > maxConcurrentResolves {
		t.Errorf("%d resolutions in flight at once, cap is %d", peak, maxConcurrentResolves)
	}
	if counter.callCount() != addrs {
		t.Errorf("resolved %d of %d addresses; the cap must slow them, not drop them",
			counter.callCount(), addrs)
	}
}

// waitForInFlight blocks until the resolver has at least n resolutions
// parked, so the peak assertion measures the cap rather than a race to
// start.
func waitForInFlight(t *testing.T, c *resolveCounter, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		got := c.inFlight
		c.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d resolutions ever went in flight, expected the cap of %d to fill", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}
