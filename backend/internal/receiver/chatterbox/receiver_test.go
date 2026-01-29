package chatterbox

import (
	"context"
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
		"min_interval": "50ms",
		"max_interval": "200ms",
		"instance":     "test-instance",
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
		"min_interval": "100us",
		"max_interval": "500us",
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
		"min_interval": "1.5ms",
		"max_interval": "2s",
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
	params := map[string]string{"min_interval": "not-a-duration"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid min_interval")
	}
}

func TestNewReceiver_InvalidMaxInterval(t *testing.T) {
	params := map[string]string{"max_interval": "not-a-duration"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid max_interval")
	}
}

func TestNewReceiver_NegativeMinInterval(t *testing.T) {
	params := map[string]string{"min_interval": "-10ms"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative min_interval")
	}
}

func TestNewReceiver_NegativeMaxInterval(t *testing.T) {
	params := map[string]string{"max_interval": "-10ms"}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative max_interval")
	}
}

func TestNewReceiver_MinExceedsMax(t *testing.T) {
	params := map[string]string{
		"min_interval": "500ms",
		"max_interval": "100ms",
	}
	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error when min > max")
	}
}

func TestNewReceiver_EqualMinMax(t *testing.T) {
	params := map[string]string{
		"min_interval": "100ms",
		"max_interval": "100ms",
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
		"min_interval": "1ms",
		"max_interval": "5ms",
		"instance":     "emit-test",
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
		"min_interval": "1s",
		"max_interval": "2s",
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
		"min_interval": "1ms",
		"max_interval": "5ms",
		"instance":     "instance-1",
	}
	params2 := map[string]string{
		"min_interval": "1ms",
		"max_interval": "5ms",
		"instance":     "instance-2",
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
		"min_interval": "10ms",
		"max_interval": "20ms",
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
		"min_interval": "50ms",
		"max_interval": "50ms",
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
