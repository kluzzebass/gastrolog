package chatterbox

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/orchestrator"
)

func TestNewReceiver_Defaults(t *testing.T) {
	r, err := NewReceiver(nil, nil)
	if err != nil {
		t.Fatalf("NewReceiver(nil) failed: %v", err)
	}
	recv := r.(*Receiver)
	if recv.minInterval != 100*time.Millisecond {
		t.Errorf("minInterval = %v, want 100ms", recv.minInterval)
	}
	if recv.maxInterval != 1*time.Second {
		t.Errorf("maxInterval = %v, want 1s", recv.maxInterval)
	}
	if recv.instance != "default" {
		t.Errorf("instance = %q, want %q", recv.instance, "default")
	}
}

func TestNewReceiver_CustomParams(t *testing.T) {
	params := map[string]string{
		"minInterval": "50ms",
		"maxInterval": "200ms",
		"instance":    "test-instance",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver(params) failed: %v", err)
	}
	recv := r.(*Receiver)
	if recv.minInterval != 50*time.Millisecond {
		t.Errorf("minInterval = %v, want 50ms", recv.minInterval)
	}
	if recv.maxInterval != 200*time.Millisecond {
		t.Errorf("maxInterval = %v, want 200ms", recv.maxInterval)
	}
	if recv.instance != "test-instance" {
		t.Errorf("instance = %q, want %q", recv.instance, "test-instance")
	}
}

func TestNewReceiver_SubMillisecond(t *testing.T) {
	params := map[string]string{
		"minInterval": "100us",
		"maxInterval": "500us",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver(params) failed: %v", err)
	}
	recv := r.(*Receiver)
	if recv.minInterval != 100*time.Microsecond {
		t.Errorf("minInterval = %v, want 100us", recv.minInterval)
	}
	if recv.maxInterval != 500*time.Microsecond {
		t.Errorf("maxInterval = %v, want 500us", recv.maxInterval)
	}
}

func TestNewReceiver_MixedUnits(t *testing.T) {
	params := map[string]string{
		"minInterval": "1.5ms",
		"maxInterval": "2s",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver(params) failed: %v", err)
	}
	recv := r.(*Receiver)
	if recv.minInterval != 1500*time.Microsecond {
		t.Errorf("minInterval = %v, want 1.5ms", recv.minInterval)
	}
	if recv.maxInterval != 2*time.Second {
		t.Errorf("maxInterval = %v, want 2s", recv.maxInterval)
	}
}

func TestNewReceiver_InvalidMinInterval(t *testing.T) {
	params := map[string]string{"minInterval": "not-a-duration"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid min_interval")
	}
}

func TestNewReceiver_InvalidMaxInterval(t *testing.T) {
	params := map[string]string{"maxInterval": "not-a-duration"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid max_interval")
	}
}

func TestNewReceiver_NegativeMinInterval(t *testing.T) {
	params := map[string]string{"minInterval": "-10ms"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative min_interval")
	}
}

func TestNewReceiver_NegativeMaxInterval(t *testing.T) {
	params := map[string]string{"maxInterval": "-10ms"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative max_interval")
	}
}

func TestNewReceiver_MinExceedsMax(t *testing.T) {
	params := map[string]string{
		"minInterval": "500ms",
		"maxInterval": "100ms",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error when min > max")
	}
}

func TestNewReceiver_EqualMinMax(t *testing.T) {
	params := map[string]string{
		"minInterval": "100ms",
		"maxInterval": "100ms",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver with min=max should succeed: %v", err)
	}
	recv := r.(*Receiver)
	if recv.minInterval != recv.maxInterval {
		t.Error("min and max should be equal")
	}
}

func TestRun_EmitsMessages(t *testing.T) {
	params := map[string]string{
		"minInterval": "1ms",
		"maxInterval": "5ms",
		"instance":    "emit-test",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
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
		if msg.Attrs["receiver"] != "chatterbox" {
			t.Errorf("message %d: receiver attr = %q, want %q", i, msg.Attrs["receiver"], "chatterbox")
		}
		if msg.Attrs["instance"] != "emit-test" {
			t.Errorf("message %d: instance attr = %q, want %q", i, msg.Attrs["instance"], "emit-test")
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
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
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

func TestRun_MultipleInstances(t *testing.T) {
	params1 := map[string]string{
		"minInterval": "1ms",
		"maxInterval": "5ms",
		"instance":    "instance-1",
	}
	params2 := map[string]string{
		"minInterval": "1ms",
		"maxInterval": "5ms",
		"instance":    "instance-2",
	}

	r1, err := NewReceiver(params1, nil)
	if err != nil {
		t.Fatalf("NewReceiver(params1) failed: %v", err)
	}
	r2, err := NewReceiver(params2, nil)
	if err != nil {
		t.Fatalf("NewReceiver(params2) failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 100)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = r1.Run(ctx, out)
	}()
	go func() {
		defer wg.Done()
		_ = r2.Run(ctx, out)
	}()

	wg.Wait()
	close(out)

	instance1Count := 0
	instance2Count := 0
	for msg := range out {
		switch msg.Attrs["instance"] {
		case "instance-1":
			instance1Count++
		case "instance-2":
			instance2Count++
		default:
			t.Errorf("unexpected instance: %q", msg.Attrs["instance"])
		}
	}

	if instance1Count == 0 {
		t.Error("instance-1 emitted no messages")
	}
	if instance2Count == 0 {
		t.Error("instance-2 emitted no messages")
	}
}

func TestRun_ReturnsNilOnCancel(t *testing.T) {
	r, err := NewReceiver(nil, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
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
	r, err := NewReceiver(map[string]string{"instance": "format-test"}, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	msg := recv.generateMessage()

	// Check attributes.
	if msg.Attrs["receiver"] != "chatterbox" {
		t.Errorf("receiver attr = %q, want %q", msg.Attrs["receiver"], "chatterbox")
	}
	if msg.Attrs["instance"] != "format-test" {
		t.Errorf("instance attr = %q, want %q", msg.Attrs["instance"], "format-test")
	}

	// Check raw contains expected fields.
	raw := string(msg.Raw)
	if len(raw) == 0 {
		t.Error("Raw is empty")
	}

	// Check IngestTS is recent.
	if time.Since(msg.IngestTS) > time.Second {
		t.Errorf("IngestTS too old: %v", msg.IngestTS)
	}
}

func TestRandomInterval_Bounds(t *testing.T) {
	r, err := NewReceiver(map[string]string{
		"minInterval": "10ms",
		"maxInterval": "20ms",
	}, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	for i := 0; i < 100; i++ {
		interval := recv.randomInterval()
		if interval < 10*time.Millisecond || interval >= 20*time.Millisecond {
			t.Errorf("interval %v out of bounds [10ms, 20ms)", interval)
		}
	}
}

func TestRandomInterval_EqualBounds(t *testing.T) {
	r, err := NewReceiver(map[string]string{
		"minInterval": "50ms",
		"maxInterval": "50ms",
	}, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	for i := 0; i < 10; i++ {
		interval := recv.randomInterval()
		if interval != 50*time.Millisecond {
			t.Errorf("interval = %v, want 50ms", interval)
		}
	}
}

func TestNewReceiver_Formats(t *testing.T) {
	params := map[string]string{
		"formats": "json,kv",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)
	if len(recv.formats) != 2 {
		t.Errorf("expected 2 formats, got %d", len(recv.formats))
	}
}

func TestNewReceiver_AllFormats(t *testing.T) {
	r, err := NewReceiver(nil, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)
	if len(recv.formats) != 6 {
		t.Errorf("expected 6 formats (all), got %d", len(recv.formats))
	}
}

func TestNewReceiver_UnknownFormat(t *testing.T) {
	params := map[string]string{
		"formats": "json,invalid",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestNewReceiver_FormatWeights(t *testing.T) {
	params := map[string]string{
		"formats":       "json,kv",
		"formatWeights": "json=10,kv=5",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)
	if recv.totalWeight != 15 {
		t.Errorf("totalWeight = %d, want 15", recv.totalWeight)
	}
	// Cumulative weights: json=10, kv=15
	if recv.weights[0] != 10 || recv.weights[1] != 15 {
		t.Errorf("weights = %v, want [10, 15]", recv.weights)
	}
}

func TestNewReceiver_InvalidWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "json",
		"formatWeights": "json=notanumber",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid weight")
	}
}

func TestNewReceiver_ZeroWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "json",
		"formatWeights": "json=0",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for zero weight")
	}
}

func TestNewReceiver_NegativeWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "json",
		"formatWeights": "json=-5",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative weight")
	}
}

func TestNewReceiver_HostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "20",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	// Generate messages and collect hosts
	hosts := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		msg := recv.generateMessage()
		if h := msg.Attrs["host"]; h != "" {
			hosts[h] = true
		}
	}

	// With 20 hosts, we should see variety
	if len(hosts) < 10 {
		t.Errorf("expected at least 10 distinct hosts with host_count=20, got %d", len(hosts))
	}
}

func TestNewReceiver_InvalidHostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "invalid",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid host_count")
	}
}

func TestNewReceiver_ZeroHostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "0",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for zero host_count")
	}
}

func TestNewReceiver_ServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "3",
		"formats":      "plain", // plain format uses service attr
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	// Generate messages and collect services
	services := make(map[string]bool)
	for i := 0; i < 500; i++ {
		msg := recv.generateMessage()
		if s := msg.Attrs["service"]; s != "" {
			services[s] = true
		}
	}

	// Should have exactly 3 distinct services
	if len(services) > 3 {
		t.Errorf("expected at most 3 distinct services with service_count=3, got %d", len(services))
	}
}

func TestNewReceiver_InvalidServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "invalid",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid service_count")
	}
}

func TestNewReceiver_ZeroServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "0",
	}
	_, err := NewReceiver(params, nil)
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
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	// Generate many messages and check for variety in raw formats
	jsonCount := 0
	accessCount := 0
	syslogCount := 0

	for i := 0; i < 1000; i++ {
		msg := recv.generateMessage()
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
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	jsonCount := 0
	for i := 0; i < 1000; i++ {
		msg := recv.generateMessage()
		raw := string(msg.Raw)
		if len(raw) > 0 && raw[0] == '{' {
			jsonCount++
		}
	}

	// With 90% weight on JSON, we should see ~900 JSON messages
	// Allow for statistical variation (should be at least 800)
	if jsonCount < 800 {
		t.Errorf("expected at least 800 JSON messages with 90%% weight, got %d", jsonCount)
	}
}

func TestGenerateMessage_AttrsIncludeReceiverAndInstance(t *testing.T) {
	params := map[string]string{
		"instance": "test-instance",
		"formats":  "json",
	}
	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("NewReceiver failed: %v", err)
	}
	recv := r.(*Receiver)

	msg := recv.generateMessage()

	// Base attrs should always be present
	if msg.Attrs["receiver"] != "chatterbox" {
		t.Errorf("receiver = %q, want %q", msg.Attrs["receiver"], "chatterbox")
	}
	if msg.Attrs["instance"] != "test-instance" {
		t.Errorf("instance = %q, want %q", msg.Attrs["instance"], "test-instance")
	}

	// Format-specific attrs should also be present (JSON format adds these)
	if msg.Attrs["service"] == "" {
		t.Error("expected service attr from format")
	}
	if msg.Attrs["host"] == "" {
		t.Error("expected host attr from format")
	}
}
