package chatterbox

import (
	"encoding/json"
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
