package chatterbox

import (
	"testing"
)

func TestNewReceiverDefaults(t *testing.T) {
	r, err := NewReceiver(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv, ok := r.(*Receiver)
	if !ok {
		t.Fatal("expected *Receiver")
	}

	if recv.minInterval != defaultMinInterval {
		t.Errorf("minInterval = %v, want %v", recv.minInterval, defaultMinInterval)
	}
	if recv.maxInterval != defaultMaxInterval {
		t.Errorf("maxInterval = %v, want %v", recv.maxInterval, defaultMaxInterval)
	}
	if recv.instance != defaultInstance {
		t.Errorf("instance = %q, want %q", recv.instance, defaultInstance)
	}
	if len(recv.formats) != len(allFormats) {
		t.Errorf("formats count = %d, want %d", len(recv.formats), len(allFormats))
	}
}

func TestNewReceiverCustomIntervals(t *testing.T) {
	params := map[string]string{
		"minInterval": "50ms",
		"maxInterval": "500ms",
	}

	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv := r.(*Receiver)
	if recv.minInterval.Milliseconds() != 50 {
		t.Errorf("minInterval = %v, want 50ms", recv.minInterval)
	}
	if recv.maxInterval.Milliseconds() != 500 {
		t.Errorf("maxInterval = %v, want 500ms", recv.maxInterval)
	}
}

func TestNewReceiverInvalidMinInterval(t *testing.T) {
	params := map[string]string{
		"minInterval": "not-a-duration",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid minInterval")
	}
}

func TestNewReceiverInvalidMaxInterval(t *testing.T) {
	params := map[string]string{
		"maxInterval": "not-a-duration",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid maxInterval")
	}
}

func TestNewReceiverNegativeInterval(t *testing.T) {
	params := map[string]string{
		"minInterval": "-1s",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative minInterval")
	}

	params = map[string]string{
		"maxInterval": "-1s",
	}

	_, err = NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative maxInterval")
	}
}

func TestNewReceiverMinExceedsMax(t *testing.T) {
	params := map[string]string{
		"minInterval": "2s",
		"maxInterval": "1s",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error when minInterval > maxInterval")
	}
}

func TestNewReceiverCustomInstance(t *testing.T) {
	params := map[string]string{
		"instance": "test-instance",
	}

	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv := r.(*Receiver)
	if recv.instance != "test-instance" {
		t.Errorf("instance = %q, want %q", recv.instance, "test-instance")
	}
}

func TestNewReceiverCustomHostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "20",
	}

	_, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewReceiverInvalidHostCount(t *testing.T) {
	params := map[string]string{
		"hostCount": "not-a-number",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid hostCount")
	}

	params = map[string]string{
		"hostCount": "0",
	}

	_, err = NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for zero hostCount")
	}

	params = map[string]string{
		"hostCount": "-1",
	}

	_, err = NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative hostCount")
	}
}

func TestNewReceiverCustomServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "15",
	}

	_, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewReceiverInvalidServiceCount(t *testing.T) {
	params := map[string]string{
		"serviceCount": "not-a-number",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid serviceCount")
	}

	params = map[string]string{
		"serviceCount": "0",
	}

	_, err = NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for zero serviceCount")
	}
}

func TestNewReceiverCustomFormats(t *testing.T) {
	params := map[string]string{
		"formats": "plain,json,kv",
	}

	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv := r.(*Receiver)
	if len(recv.formats) != 3 {
		t.Errorf("formats count = %d, want 3", len(recv.formats))
	}
}

func TestNewReceiverSingleFormat(t *testing.T) {
	params := map[string]string{
		"formats": "json",
	}

	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv := r.(*Receiver)
	if len(recv.formats) != 1 {
		t.Errorf("formats count = %d, want 1", len(recv.formats))
	}
}

func TestNewReceiverUnknownFormat(t *testing.T) {
	params := map[string]string{
		"formats": "plain,unknown,json",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestNewReceiverEmptyFormatsString(t *testing.T) {
	params := map[string]string{
		"formats": "   ",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for empty formats string")
	}
}

func TestNewReceiverDuplicateFormats(t *testing.T) {
	params := map[string]string{
		"formats": "plain,json,plain,json",
	}

	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv := r.(*Receiver)
	if len(recv.formats) != 2 {
		t.Errorf("formats count = %d, want 2 (duplicates removed)", len(recv.formats))
	}
}

func TestNewReceiverCustomWeights(t *testing.T) {
	params := map[string]string{
		"formats":       "plain,json,kv",
		"formatWeights": "plain=30,json=20,kv=50",
	}

	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv := r.(*Receiver)
	if recv.totalWeight != 100 {
		t.Errorf("totalWeight = %d, want 100", recv.totalWeight)
	}
}

func TestNewReceiverPartialWeights(t *testing.T) {
	// Only specify weight for some formats; others get default weight of 1.
	params := map[string]string{
		"formats":       "plain,json,kv",
		"formatWeights": "plain=10",
	}

	r, err := NewReceiver(params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recv := r.(*Receiver)
	// plain=10, json=1, kv=1 => total=12
	if recv.totalWeight != 12 {
		t.Errorf("totalWeight = %d, want 12", recv.totalWeight)
	}
}

func TestNewReceiverInvalidWeightFormat(t *testing.T) {
	params := map[string]string{
		"formats":       "plain,json",
		"formatWeights": "plain:10", // Wrong separator.
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for invalid weight format")
	}
}

func TestNewReceiverInvalidWeightValue(t *testing.T) {
	params := map[string]string{
		"formats":       "plain,json",
		"formatWeights": "plain=abc",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for non-numeric weight")
	}
}

func TestNewReceiverZeroWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "plain,json",
		"formatWeights": "plain=0",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for zero weight")
	}
}

func TestNewReceiverNegativeWeight(t *testing.T) {
	params := map[string]string{
		"formats":       "plain,json",
		"formatWeights": "plain=-5",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for negative weight")
	}
}

func TestNewReceiverUnknownFormatInWeights(t *testing.T) {
	params := map[string]string{
		"formats":       "plain,json",
		"formatWeights": "unknown=10",
	}

	_, err := NewReceiver(params, nil)
	if err == nil {
		t.Error("expected error for unknown format in weights")
	}
}

func TestParseFormats(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{"empty returns all", "", len(allFormats), false},
		{"single format", "plain", 1, false},
		{"multiple formats", "plain,json,kv", 3, false},
		{"with whitespace", " plain , json , kv ", 3, false},
		{"unknown format", "plain,unknown", 0, true},
		{"only whitespace", "   ", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFormats(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFormats(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(got) != tt.want {
				t.Errorf("parseFormats(%q) = %d formats, want %d", tt.input, len(got), tt.want)
			}
		})
	}
}

func TestIsValidFormat(t *testing.T) {
	for _, f := range allFormats {
		if !isValidFormat(f) {
			t.Errorf("isValidFormat(%q) = false, want true", f)
		}
	}

	if isValidFormat("unknown") {
		t.Error("isValidFormat(\"unknown\") = true, want false")
	}
}
