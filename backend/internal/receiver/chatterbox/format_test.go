package chatterbox

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

func newTestRng() *rand.Rand {
	return rand.New(rand.NewPCG(12345, 67890))
}

func newTestPools() *AttributePools {
	return NewAttributePools(10, 5)
}

func TestPlainTextFormat_Generate(t *testing.T) {
	pools := newTestPools()
	format := NewPlainTextFormat(pools)
	rng := newTestRng()

	for i := 0; i < 100; i++ {
		raw, attrs := format.Generate(rng)
		if len(raw) == 0 {
			t.Errorf("iteration %d: raw is empty", i)
		}
		if attrs["service"] == "" {
			t.Errorf("iteration %d: missing service attr", i)
		}
		if attrs["host"] == "" {
			t.Errorf("iteration %d: missing host attr", i)
		}
	}
}

func TestKeyValueFormat_Generate(t *testing.T) {
	pools := newTestPools()
	format := NewKeyValueFormat(pools)
	rng := newTestRng()

	for i := 0; i < 100; i++ {
		raw, attrs := format.Generate(rng)
		if len(raw) == 0 {
			t.Errorf("iteration %d: raw is empty", i)
		}
		// Check that it contains key=value patterns
		line := string(raw)
		if !strings.Contains(line, "=") {
			t.Errorf("iteration %d: expected key=value format, got: %s", i, line)
		}
		if !strings.Contains(line, "level=") {
			t.Errorf("iteration %d: expected level= in output, got: %s", i, line)
		}
		if attrs["service"] == "" {
			t.Errorf("iteration %d: missing service attr", i)
		}
		if attrs["env"] == "" {
			t.Errorf("iteration %d: missing env attr", i)
		}
		if attrs["host"] == "" {
			t.Errorf("iteration %d: missing host attr", i)
		}
	}
}

func TestJSONFormat_Generate(t *testing.T) {
	pools := newTestPools()
	format := NewJSONFormat(pools)
	rng := newTestRng()

	for i := 0; i < 100; i++ {
		raw, attrs := format.Generate(rng)
		if len(raw) == 0 {
			t.Errorf("iteration %d: raw is empty", i)
		}
		// Check that it's valid JSON
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Errorf("iteration %d: invalid JSON: %v", i, err)
		}
		// Check required fields
		if _, ok := obj["level"]; !ok {
			t.Errorf("iteration %d: missing level field", i)
		}
		if _, ok := obj["msg"]; !ok {
			t.Errorf("iteration %d: missing msg field", i)
		}
		if attrs["service"] == "" {
			t.Errorf("iteration %d: missing service attr", i)
		}
		if attrs["env"] == "" {
			t.Errorf("iteration %d: missing env attr", i)
		}
		if attrs["host"] == "" {
			t.Errorf("iteration %d: missing host attr", i)
		}
	}
}

func TestAccessLogFormat_Generate(t *testing.T) {
	pools := newTestPools()
	format := NewAccessLogFormat(pools)
	rng := newTestRng()

	for i := 0; i < 100; i++ {
		raw, attrs := format.Generate(rng)
		if len(raw) == 0 {
			t.Errorf("iteration %d: raw is empty", i)
		}
		line := string(raw)
		// Check for access log structure
		if !strings.Contains(line, " - ") {
			t.Errorf("iteration %d: expected combined log format, got: %s", i, line)
		}
		if !strings.Contains(line, "HTTP/") {
			t.Errorf("iteration %d: expected HTTP protocol in log, got: %s", i, line)
		}
		if attrs["service"] != "nginx" {
			t.Errorf("iteration %d: expected service=nginx, got %s", i, attrs["service"])
		}
		if attrs["vhost"] == "" {
			t.Errorf("iteration %d: missing vhost attr", i)
		}
		if attrs["host"] == "" {
			t.Errorf("iteration %d: missing host attr", i)
		}
	}
}

func TestSyslogFormat_Generate(t *testing.T) {
	pools := newTestPools()
	format := NewSyslogFormat(pools)
	rng := newTestRng()

	for i := 0; i < 100; i++ {
		raw, attrs := format.Generate(rng)
		if len(raw) == 0 {
			t.Errorf("iteration %d: raw is empty", i)
		}
		line := string(raw)
		// Check for syslog structure - starts with priority
		if !strings.HasPrefix(line, "<") {
			t.Errorf("iteration %d: expected syslog priority prefix, got: %s", i, line)
		}
		if attrs["service"] == "" {
			t.Errorf("iteration %d: missing service attr", i)
		}
		if attrs["facility"] == "" {
			t.Errorf("iteration %d: missing facility attr", i)
		}
		if attrs["host"] == "" {
			t.Errorf("iteration %d: missing host attr", i)
		}
	}
}

func TestWeirdFormat_Generate(t *testing.T) {
	pools := newTestPools()
	format := NewWeirdFormat(pools)
	rng := newTestRng()

	for i := 0; i < 100; i++ {
		raw, attrs := format.Generate(rng)
		// Weird format may produce empty data in some cases, but attrs should always be set
		if attrs["service"] != "unknown" {
			t.Errorf("iteration %d: expected service=unknown, got %s", i, attrs["service"])
		}
		if attrs["host"] == "" {
			t.Errorf("iteration %d: missing host attr", i)
		}
		// At least verify it doesn't panic
		_ = raw
	}
}

func TestAttributePools_HostCount(t *testing.T) {
	pools := NewAttributePools(5, 3)
	if len(pools.Hosts) != 5 {
		t.Errorf("expected 5 hosts, got %d", len(pools.Hosts))
	}
	if len(pools.Services) != 3 {
		t.Errorf("expected 3 services, got %d", len(pools.Services))
	}
}

func TestAttributePools_HostNames(t *testing.T) {
	pools := NewAttributePools(3, 2)
	expected := []string{"host-1", "host-2", "host-3"}
	for i, want := range expected {
		if pools.Hosts[i] != want {
			t.Errorf("Hosts[%d] = %q, want %q", i, pools.Hosts[i], want)
		}
	}
}

func TestFormat_AttributeVariation(t *testing.T) {
	pools := newTestPools()
	format := NewKeyValueFormat(pools)
	rng := newTestRng()

	hosts := make(map[string]bool)
	services := make(map[string]bool)

	for i := 0; i < 1000; i++ {
		_, attrs := format.Generate(rng)
		hosts[attrs["host"]] = true
		services[attrs["service"]] = true
	}

	// With 10 hosts and 1000 iterations, we should see at least 5 distinct hosts
	if len(hosts) < 5 {
		t.Errorf("expected at least 5 distinct hosts, got %d", len(hosts))
	}
	// With 5 services, we should see at least 3
	if len(services) < 3 {
		t.Errorf("expected at least 3 distinct services, got %d", len(services))
	}
}

func TestFormat_SameAttrsStableSourceID(t *testing.T) {
	pools := newTestPools()
	format := NewPlainTextFormat(pools)

	// Use fixed seed for reproducible results
	rng1 := rand.New(rand.NewPCG(1, 2))
	rng2 := rand.New(rand.NewPCG(1, 2))

	// Same seed should produce same attrs
	_, attrs1 := format.Generate(rng1)
	_, attrs2 := format.Generate(rng2)

	if attrs1["host"] != attrs2["host"] {
		t.Errorf("same seed should produce same host: %q vs %q", attrs1["host"], attrs2["host"])
	}
	if attrs1["service"] != attrs2["service"] {
		t.Errorf("same seed should produce same service: %q vs %q", attrs1["service"], attrs2["service"])
	}
}

// Additional format-specific tests

func TestJSONFormat_AllVariants(t *testing.T) {
	// Test that all JSON format variants (HTTP metrics, error details, business event,
	// system metrics, distributed tracing) can be generated without panicking.
	pools := newTestPools()
	format := NewJSONFormat(pools)

	// Use enough iterations to hit all variants (5 variants, each has ~20% chance).
	// With 500 iterations, extremely unlikely to miss any variant.
	variantFields := map[string]bool{
		"method":      false, // HTTP metrics
		"error":       false, // Error details
		"event_type":  false, // Business event
		"cpu_percent": false, // System metrics
		"trace_id":    false, // Distributed tracing
	}

	for i := 0; i < 500; i++ {
		rng := rand.New(rand.NewPCG(uint64(i), uint64(i+1)))
		raw, _ := format.Generate(rng)

		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("iteration %d: invalid JSON: %v", i, err)
		}

		for field := range variantFields {
			if _, ok := obj[field]; ok {
				variantFields[field] = true
			}
		}
	}

	for field, seen := range variantFields {
		if !seen {
			t.Errorf("variant with field %q was never generated", field)
		}
	}
}

func TestKeyValueFormat_AllVariants(t *testing.T) {
	// Test that all KV format variants can be generated.
	pools := newTestPools()
	format := NewKeyValueFormat(pools)

	// Check for distinctive patterns from each variant.
	variantPatterns := map[string]bool{
		"method=":   false, // HTTP request style
		"table=":    false, // Database query style
		"user_id=":  false, // User action style
		"trace_id=": false, // Generic with trace context
	}

	for i := 0; i < 500; i++ {
		rng := rand.New(rand.NewPCG(uint64(i), uint64(i+1)))
		raw, _ := format.Generate(rng)
		line := string(raw)

		for pattern := range variantPatterns {
			if strings.Contains(line, pattern) {
				variantPatterns[pattern] = true
			}
		}
	}

	for pattern, seen := range variantPatterns {
		if !seen {
			t.Errorf("variant with pattern %q was never generated", pattern)
		}
	}
}

func TestSyslogFormat_PriorityRange(t *testing.T) {
	// Test that syslog messages have valid priority values (0-191).
	pools := newTestPools()
	format := NewSyslogFormat(pools)
	rng := newTestRng()

	for i := 0; i < 100; i++ {
		raw, _ := format.Generate(rng)
		line := string(raw)

		// Extract priority: <N>...
		if !strings.HasPrefix(line, "<") {
			t.Errorf("iteration %d: missing priority prefix", i)
			continue
		}

		endIdx := strings.Index(line, ">")
		if endIdx == -1 {
			t.Errorf("iteration %d: missing priority suffix", i)
			continue
		}

		var priority int
		if _, err := fmt.Sscanf(line[1:endIdx], "%d", &priority); err != nil {
			t.Errorf("iteration %d: failed to parse priority: %v", i, err)
			continue
		}

		// Priority = Facility * 8 + Severity
		// Max facility = 23, max severity = 7, so max priority = 23*8+7 = 191
		if priority < 0 || priority > 191 {
			t.Errorf("iteration %d: priority %d out of range [0, 191]", i, priority)
		}
	}
}

func TestAccessLogFormat_CombinedFormat(t *testing.T) {
	// Verify access log follows combined log format structure.
	pools := newTestPools()
	format := NewAccessLogFormat(pools)
	rng := newTestRng()

	for i := 0; i < 50; i++ {
		raw, _ := format.Generate(rng)
		line := string(raw)

		// Combined log format: IP - USER [DATE] "METHOD PATH PROTOCOL" STATUS SIZE "REFERER" "USER-AGENT"
		// Check for key markers.
		if !strings.Contains(line, " - ") {
			t.Errorf("iteration %d: missing ' - ' separator", i)
		}
		if !strings.Contains(line, "[") || !strings.Contains(line, "]") {
			t.Errorf("iteration %d: missing timestamp brackets", i)
		}
		if !strings.Contains(line, "\"") {
			t.Errorf("iteration %d: missing quoted sections", i)
		}
		// Check for HTTP method.
		methods := []string{"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"}
		hasMethod := false
		for _, m := range methods {
			if strings.Contains(line, "\""+m+" ") {
				hasMethod = true
				break
			}
		}
		if !hasMethod {
			t.Errorf("iteration %d: missing HTTP method in log: %s", i, line)
		}
	}
}

func TestWeirdFormat_Robustness(t *testing.T) {
	// Weird format should always produce valid attrs and not panic.
	pools := newTestPools()
	format := NewWeirdFormat(pools)

	for i := 0; i < 200; i++ {
		rng := rand.New(rand.NewPCG(uint64(i), uint64(i+1)))

		// Should not panic.
		raw, attrs := format.Generate(rng)

		// Raw may be empty, but should never be nil.
		if raw == nil {
			t.Errorf("iteration %d: raw is nil", i)
		}

		// Attrs should always have required fields.
		if attrs == nil {
			t.Errorf("iteration %d: attrs is nil", i)
			continue
		}
		if attrs["service"] == "" {
			t.Errorf("iteration %d: missing service attr", i)
		}
		if attrs["host"] == "" {
			t.Errorf("iteration %d: missing host attr", i)
		}
	}
}
