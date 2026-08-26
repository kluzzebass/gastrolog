package otlp

import (
	"strconv"
	"strings"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"gastrolog/internal/ingester/limits"
	"gastrolog/internal/logging/logtest"
)

func stringKV(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

// TestAttributeFloodIsBounded proves a record carrying tens of thousands of
// attributes cannot explode the attribute map — or, downstream, index
// cardinality. The log line itself still lands: attributes are dropped, the
// record is not.
func TestAttributeFloodIsBounded(t *testing.T) {
	t.Parallel()
	logger, logs := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	flood := make([]*commonpb.KeyValue, 50_000)
	for i := range flood {
		flood[i] = stringKV("k"+strconv.Itoa(i), "v")
	}
	record := &logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "the log line"}},
		Attributes: flood,
	}

	msg := ing.logRecordToMessage(record, nil, nil, time.Now())

	// Count only the producer's attributes: the ingester's own few
	// (ingester_type, severity, trace ids) sit outside the budget.
	fromProducer := 0
	for k := range msg.Attrs {
		if strings.HasPrefix(k, "k") {
			fromProducer++
		}
	}
	if fromProducer > limits.Records.Count {
		t.Errorf("stored %d producer attributes, past the %d ceiling", fromProducer, limits.Records.Count)
	}
	if string(msg.Raw) != "the log line" {
		t.Errorf("the record's payload was lost: %q", msg.Raw)
	}
	if !logs.Contains("OTLP record attributes dropped", "max_attrs") {
		t.Errorf("attributes were dropped silently: %s", logs)
	}
}

// TestOversizeAttributeValueIsDroppedNotStored proves one enormous value
// cannot slip past the ceiling by arriving alone.
func TestOversizeAttributeValueIsDroppedNotStored(t *testing.T) {
	t.Parallel()
	logger, _ := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	huge := strings.Repeat("x", limits.Records.ValueBytes+1)
	record := &logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "line"}},
		Attributes: []*commonpb.KeyValue{stringKV("stack", huge)},
	}

	msg := ing.logRecordToMessage(record, nil, nil, time.Now())

	if _, ok := msg.Attrs["stack"]; ok {
		t.Errorf("an oversize value was stored: %d bytes", len(msg.Attrs["stack"]))
	}
}

// TestConformingAttributesAreUntouched proves the ceiling did not change
// what a normal collector's records look like, including resource and scope
// precedence and a multi-kilobyte stack trace.
func TestConformingAttributesAreUntouched(t *testing.T) {
	t.Parallel()
	logger, logs := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	resourceAttrs := map[string]string{"service.name": "api", "host.name": "node-1"}
	scopeAttrs := map[string]string{"scope": "http"}
	stack := strings.Repeat("frame\n", 500)
	record := &logspb.LogRecord{
		Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "line"}},
		Attributes: []*commonpb.KeyValue{
			stringKV("exception.stacktrace", stack),
			stringKV("host.name", "node-2"), // record attrs win over resource
		},
	}

	msg := ing.logRecordToMessage(record, resourceAttrs, scopeAttrs, time.Now())

	if msg.Attrs["service.name"] != "api" {
		t.Errorf("resource attribute lost: %q", msg.Attrs["service.name"])
	}
	if msg.Attrs["scope"] != "http" {
		t.Errorf("scope attribute lost: %q", msg.Attrs["scope"])
	}
	if msg.Attrs["host.name"] != "node-2" {
		t.Errorf("record attribute did not win over resource: %q", msg.Attrs["host.name"])
	}
	if msg.Attrs["exception.stacktrace"] != stack {
		t.Errorf("a %d-byte stack trace was truncated or dropped", len(stack))
	}
	if logs.Contains("attributes dropped") {
		t.Errorf("conforming attributes were reported as dropped: %s", logs)
	}
}
