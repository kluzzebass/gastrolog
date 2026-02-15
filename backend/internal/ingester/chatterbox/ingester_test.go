package chatterbox

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

func TestNewIngester_Defaults(t *testing.T) {
	r, err := NewIngester(uuid.New(), nil, nil)
	if err != nil {
		t.Fatalf("NewIngester(nil) failed: %v", err)
	}
	recv := r.(*Ingester)
	if recv.minInterval != 100*time.Millisecond {
		t.Errorf("minInterval = %v, want 100ms", recv.minInterval)
	}
	if recv.maxInterval != 1*time.Second {
		t.Errorf("maxInterval = %v, want 1s", recv.maxInterval)
	}
}

func TestNewIngester_CustomParams(t *testing.T) {
	params := map[string]string{
		"minInterval": "50ms",
		"maxInterval": "200ms",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester(params) failed: %v", err)
	}
	recv := r.(*Ingester)
	if recv.minInterval != 50*time.Millisecond {
		t.Errorf("minInterval = %v, want 50ms", recv.minInterval)
	}
	if recv.maxInterval != 200*time.Millisecond {
		t.Errorf("maxInterval = %v, want 200ms", recv.maxInterval)
	}
}

func TestNewIngester_SubMillisecond(t *testing.T) {
	params := map[string]string{
		"minInterval": "100us",
		"maxInterval": "500us",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester(params) failed: %v", err)
	}
	recv := r.(*Ingester)
	if recv.minInterval != 100*time.Microsecond {
		t.Errorf("minInterval = %v, want 100us", recv.minInterval)
	}
	if recv.maxInterval != 500*time.Microsecond {
		t.Errorf("maxInterval = %v, want 500us", recv.maxInterval)
	}
}

func TestNewIngester_MixedUnits(t *testing.T) {
	params := map[string]string{
		"minInterval": "1.5ms",
		"maxInterval": "2s",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester(params) failed: %v", err)
	}
	recv := r.(*Ingester)
	if recv.minInterval != 1500*time.Microsecond {
		t.Errorf("minInterval = %v, want 1.5ms", recv.minInterval)
	}
	if recv.maxInterval != 2*time.Second {
		t.Errorf("maxInterval = %v, want 2s", recv.maxInterval)
	}
}

func TestNewIngester_InvalidMinInterval(t *testing.T) {
	params := map[string]string{"minInterval": "not-a-duration"}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for invalid min_interval")
	}
}

func TestNewIngester_InvalidMaxInterval(t *testing.T) {
	params := map[string]string{"maxInterval": "not-a-duration"}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for invalid max_interval")
	}
}

func TestNewIngester_NegativeMinInterval(t *testing.T) {
	params := map[string]string{"minInterval": "-10ms"}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for negative min_interval")
	}
}

func TestNewIngester_NegativeMaxInterval(t *testing.T) {
	params := map[string]string{"maxInterval": "-10ms"}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for negative max_interval")
	}
}

func TestNewIngester_MinExceedsMax(t *testing.T) {
	params := map[string]string{
		"minInterval": "500ms",
		"maxInterval": "100ms",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error when min > max")
	}
}

func TestNewIngester_EqualMinMax(t *testing.T) {
	params := map[string]string{
		"minInterval": "100ms",
		"maxInterval": "100ms",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester with min=max should succeed: %v", err)
	}
	recv := r.(*Ingester)
	if recv.minInterval != recv.maxInterval {
		t.Error("min and max should be equal")
	}
}

func TestRun_EmitsMessages(t *testing.T) {
	params := map[string]string{
		"minInterval": "1ms",
		"maxInterval": "5ms",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 100)

	var runErr error
	done := make(chan struct{})
	go func() {
		runErr = r.Run(ctx, out)
		close(done)
	}()

	<-done
	close(out)

	if runErr != nil {
		t.Errorf("Run returned error: %v", runErr)
	}

	var messages []orchestrator.IngestMessage
	for msg := range out {
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		t.Error("expected at least one message")
	}

	for i, msg := range messages {
		if msg.Attrs["ingester_type"] != "chatterbox" {
			t.Errorf("message %d: ingester attr = %q, want %q", i, msg.Attrs["ingester_type"], "chatterbox")
		}
		if len(msg.Raw) == 0 {
			t.Errorf("message %d: Raw is empty", i)
		}
		if msg.IngestTS.IsZero() {
			t.Errorf("message %d: IngestTS is zero", i)
		}
	}
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	params := map[string]string{
		"minInterval": "1s",
		"maxInterval": "2s",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 10)

	done := make(chan struct{})
	go func() {
		_ = r.Run(ctx, out)
		close(done)
	}()

	// Cancel immediately - Run should exit promptly without waiting for interval.
	cancel()

	select {
	case <-done:
		// Success - Run exited promptly.
	case <-time.After(100 * time.Millisecond):
		t.Error("Run did not stop promptly after context cancellation")
	}
}

func TestRun_ConcurrentIngesters(t *testing.T) {
	params := map[string]string{
		"minInterval": "1ms",
		"maxInterval": "5ms",
	}

	r1, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester(r1) failed: %v", err)
	}
	r2, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester(r2) failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 100)

	var wg sync.WaitGroup
	wg.Go(func() { _ = r1.Run(ctx, out) })
	wg.Go(func() { _ = r2.Run(ctx, out) })
	wg.Wait()
	close(out)

	count := 0
	for range out {
		count++
	}

	if count < 2 {
		t.Errorf("expected at least 2 messages from concurrent ingesters, got %d", count)
	}
}

func TestRun_ReturnsNilOnCancel(t *testing.T) {
	r, err := NewIngester(uuid.New(), nil, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 10)

	var runErr error
	done := make(chan struct{})
	go func() {
		runErr = r.Run(ctx, out)
		close(done)
	}()

	cancel()
	<-done

	if runErr != nil {
		t.Errorf("Run should return nil on context cancellation, got: %v", runErr)
	}
}

func TestGenerateMessage_Format(t *testing.T) {
	r, err := NewIngester(uuid.New(), nil, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	msgs := recv.generateMessages()
	if len(msgs) == 0 {
		t.Fatal("generateMessages returned no messages")
	}
	msg := msgs[0]

	if msg.Attrs["ingester_type"] != "chatterbox" {
		t.Errorf("ingester attr = %q, want %q", msg.Attrs["ingester_type"], "chatterbox")
	}

	if len(msg.Raw) == 0 {
		t.Error("Raw is empty")
	}

	if time.Since(msg.IngestTS) > time.Second {
		t.Errorf("IngestTS too old: %v", msg.IngestTS)
	}
}

func TestRandomInterval_Bounds(t *testing.T) {
	r, err := NewIngester(uuid.New(), map[string]string{
		"minInterval": "10ms",
		"maxInterval": "20ms",
	}, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	for range 100 {
		interval := recv.randomInterval()
		if interval < 10*time.Millisecond || interval >= 20*time.Millisecond {
			t.Errorf("interval %v out of bounds [10ms, 20ms)", interval)
		}
	}
}

func TestRandomInterval_EqualBounds(t *testing.T) {
	r, err := NewIngester(uuid.New(), map[string]string{
		"minInterval": "50ms",
		"maxInterval": "50ms",
	}, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	for range 10 {
		interval := recv.randomInterval()
		if interval != 50*time.Millisecond {
			t.Errorf("interval = %v, want 50ms", interval)
		}
	}
}

func TestNewIngester_Formats(t *testing.T) {
	params := map[string]string{
		"formats": "json,kv",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)
	if len(recv.formats) != 2 {
		t.Errorf("expected 2 formats, got %d", len(recv.formats))
	}
}

func TestNewIngester_AllFormats(t *testing.T) {
	r, err := NewIngester(uuid.New(), nil, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)
	if len(recv.formats) != 7 {
		t.Errorf("expected 7 formats (all), got %d", len(recv.formats))
	}
}

func TestGenerateMessages_MultirecordProducesMultiple(t *testing.T) {
	params := map[string]string{
		"formats": "multirecord",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	multiCount := 0
	for i := range 100 {
		msgs := recv.generateMessages()
		if len(msgs) > 1 {
			multiCount++
			// All messages from one multirecord burst should have same format attr
			format := msgs[0].Attrs["format"]
			for _, m := range msgs {
				if m.Attrs["format"] != format {
					t.Errorf("burst %d: inconsistent format attr", i)
				}
			}
		}
	}
	if multiCount < 50 {
		t.Errorf("expected multirecord to produce multiple messages most of the time, got %d/100 bursts", multiCount)
	}
}

func TestNewIngester_UnknownFormat(t *testing.T) {
	params := map[string]string{
		"formats": "json,invalid",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestNewIngester_FormatWeights(t *testing.T) {
	params := map[string]string{
		"formats":       "json,kv",
		"formatWeights": "json=10,kv=5",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)
	if recv.totalWeight != 15 {
		t.Errorf("totalWeight = %d, want 15", recv.totalWeight)
	}
	// Cumulative weights: json=10, kv=15
	if recv.weights[0] != 10 || recv.weights[1] != 15 {
		t.Errorf("weights = %v, want [10, 15]", recv.weights)
	}
}

func TestNewIngester_InvalidWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "json",
		"formatWeights": "json=notanumber",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for invalid weight")
	}
}

func TestNewIngester_ZeroWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "json",
		"formatWeights": "json=0",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for zero weight")
	}
}

func TestNewIngester_NegativeWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "json",
		"formatWeights": "json=-5",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for negative weight")
	}
}

func TestNewIngester_HostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "20",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	// Generate messages and collect hosts
	hosts := make(map[string]bool)
	for range 1000 {
		for _, msg := range recv.generateMessages() {
			if h := msg.Attrs["host"]; h != "" {
				hosts[h] = true
			}
		}
	}

	// With 20 hosts, we should see variety
	if len(hosts) < 10 {
		t.Errorf("expected at least 10 distinct hosts with host_count=20, got %d", len(hosts))
	}
}

func TestNewIngester_InvalidHostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "invalid",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for invalid host_count")
	}
}

func TestNewIngester_ZeroHostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "0",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for zero host_count")
	}
}

func TestNewIngester_ServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "3",
		"formats":      "plain", // plain format uses service attr
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	// Generate messages and collect services
	services := make(map[string]bool)
	for range 500 {
		for _, msg := range recv.generateMessages() {
			if s := msg.Attrs["service"]; s != "" {
				services[s] = true
			}
		}
	}

	// Should have exactly 3 distinct services
	if len(services) > 3 {
		t.Errorf("expected at most 3 distinct services with service_count=3, got %d", len(services))
	}
}

func TestNewIngester_InvalidServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "invalid",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for invalid service_count")
	}
}

func TestNewIngester_ZeroServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "0",
	}
	_, err := NewIngester(uuid.New(), params, nil)
	if err == nil {
		t.Error("expected error for zero service_count")
	}
}

func TestGenerateMessage_MultipleFormats(t *testing.T) {
	params := map[string]string{
		"minInterval": "1ms",
		"maxInterval": "5ms",
		"formats":     "plain,json,kv,access,syslog",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	// Generate many messages and check for variety in raw formats
	jsonCount := 0
	accessCount := 0
	syslogCount := 0

	for range 1000 {
		for _, msg := range recv.generateMessages() {
			raw := string(msg.Raw)
			if len(raw) > 0 && raw[0] == '{' {
				jsonCount++
			}
			if strings.Contains(raw, "HTTP/") {
				accessCount++
			}
			if len(raw) > 0 && raw[0] == '<' {
				syslogCount++
			}
		}
	}

	// With equal weights and 5 formats, each should get ~200 hits
	// Allow for statistical variation
	if jsonCount < 50 {
		t.Errorf("expected at least 50 JSON messages, got %d", jsonCount)
	}
	if accessCount < 50 {
		t.Errorf("expected at least 50 access log messages, got %d", accessCount)
	}
	if syslogCount < 50 {
		t.Errorf("expected at least 50 syslog messages, got %d", syslogCount)
	}
}

func TestGenerateMessage_WeightedSelection(t *testing.T) {
	params := map[string]string{
		"formats":       "json,plain",
		"formatWeights": "json=90,plain=10",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	jsonCount := 0
	for range 1000 {
		for _, msg := range recv.generateMessages() {
			raw := string(msg.Raw)
			if len(raw) > 0 && raw[0] == '{' {
				jsonCount++
			}
		}
	}

	// With 90% weight on JSON, we should see ~900 JSON messages
	// Allow for statistical variation (should be at least 800)
	if jsonCount < 800 {
		t.Errorf("expected at least 800 JSON messages with 90%% weight, got %d", jsonCount)
	}
}

func TestGenerateMessage_AttrsIncludeIngesterType(t *testing.T) {
	params := map[string]string{
		"formats": "json",
	}
	r, err := NewIngester(uuid.New(), params, nil)
	if err != nil {
		t.Fatalf("NewIngester failed: %v", err)
	}
	recv := r.(*Ingester)

	msgs := recv.generateMessages()
	if len(msgs) == 0 {
		t.Fatal("generateMessages returned no messages")
	}
	msg := msgs[0]

	if msg.Attrs["ingester_type"] != "chatterbox" {
		t.Errorf("ingester = %q, want %q", msg.Attrs["ingester_type"], "chatterbox")
	}

	// Format-specific attrs should also be present (JSON format adds these)
	if msg.Attrs["service"] == "" {
		t.Error("expected service attr from format")
	}
	if msg.Attrs["host"] == "" {
		t.Error("expected host attr from format")
	}
}
