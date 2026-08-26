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
//
// It also pins WHICH attributes survive. Dropping a map-ordered subset would
// mean two identical records ingest differently, and the same record
// differently on the next process; the survivors are the first Count in
// protobuf order, every time.
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
	if fromProducer != limits.Records.Count {
		t.Errorf("stored %d producer attributes, want exactly the %d the ceiling allows", fromProducer, limits.Records.Count)
	}
	for i := range limits.Records.Count {
		if _, ok := msg.Attrs["k"+strconv.Itoa(i)]; !ok {
			t.Fatalf("k%d was dropped; the survivors are not the first %d in protobuf order", i, limits.Records.Count)
		}
	}
	if _, ok := msg.Attrs["k"+strconv.Itoa(limits.Records.Count)]; ok {
		t.Errorf("k%d survived past the ceiling", limits.Records.Count)
	}
	if string(msg.Raw) != "the log line" {
		t.Errorf("the record's payload was lost: %q", msg.Raw)
	}
	if !logs.Contains("OTLP record attributes dropped", "max_attrs") {
		t.Errorf("attributes were dropped silently: %s", logs)
	}

	// Same input, same survivors — the second pass must agree with the
	// first, or the vault sees two different records for one input.
	again := ing.logRecordToMessage(record, nil, nil, time.Now())
	if len(again.Attrs) != len(msg.Attrs) {
		t.Fatalf("attribute count differed between runs: %d then %d", len(msg.Attrs), len(again.Attrs))
	}
	for k, v := range msg.Attrs {
		if again.Attrs[k] != v {
			t.Fatalf("attribute %q survived one pass and not the other", k)
		}
	}
}

// TestRecordAttributesWinTheBudgetOverResource proves the ceiling spends its
// budget on the most specific level first: a resource-level flood must not
// starve out the record's own attributes, which are the ones describing this
// particular line.
func TestRecordAttributesWinTheBudgetOverResource(t *testing.T) {
	t.Parallel()
	logger, _ := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	flood := make([]*commonpb.KeyValue, 50_000)
	for i := range flood {
		flood[i] = stringKV("resource"+strconv.Itoa(i), "v")
	}
	record := &logspb.LogRecord{
		Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "line"}},
		Attributes: []*commonpb.KeyValue{stringKV("request_id", "abc123")},
	}

	msg := ing.logRecordToMessage(record, flood, nil, time.Now())

	if msg.Attrs["request_id"] != "abc123" {
		t.Errorf("a resource-level flood starved out the record's own attribute: %v", msg.Attrs["request_id"])
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

// TestAttributesCannotForgeTheIngestersOwnAttributes proves a producer
// cannot overwrite the attributes that describe the record itself — and that
// an attribute displaced this way is reported rather than vanishing. Every
// one of these names is a plausible OTLP attribute, "severity" most of all,
// so this is a real record losing real attributes, not only an attack.
func TestAttributesCannotForgeTheIngestersOwnAttributes(t *testing.T) {
	t.Parallel()
	logger, logs := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	ts := time.Unix(1700000000, 0)
	record := &logspb.LogRecord{
		Body:                 &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "line"}},
		SeverityText:         "ERROR",
		SeverityNumber:       logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		TraceId:              []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:               []byte{1, 2, 3, 4, 5, 6, 7, 8},
		TimeUnixNano:         uint64(ts.UnixNano()),
		ObservedTimeUnixNano: uint64(ts.UnixNano()),
		Attributes: []*commonpb.KeyValue{
			stringKV("severity", "DEBUG"),
			stringKV("severity_number", "1"),
			stringKV("trace_id", "deadbeef"),
			stringKV("span_id", "deadbeef"),
			stringKV("ingester_type", "syslog"),
			stringKV("time_unix_nano", "not a time"),
			stringKV("observed_ts", "not a time"),
		},
	}

	msg := ing.logRecordToMessage(record, nil, nil, time.Now())

	for key, want := range map[string]string{
		"severity":        "ERROR",
		"severity_number": strconv.Itoa(int(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR)),
		"trace_id":        "0102030405060708090a0b0c0d0e0f10",
		"span_id":         "0102030405060708",
		"ingester_type":   "otlp",
		"time_unix_nano":  ts.Format(time.RFC3339Nano),
		"observed_ts":     ts.Format(time.RFC3339Nano),
	} {
		if msg.Attrs[key] != want {
			t.Errorf("an attribute forged %s: got %q, want %q", key, msg.Attrs[key], want)
		}
	}

	// All seven are written after the producer's attributes, so all seven
	// displaced one here.
	if !logs.Contains("OTLP record attributes dropped", "displaced=7") {
		t.Errorf("displaced attributes vanished without a word: %s", logs)
	}
}

// TestUncontestedAttributesAreNotReportedAsDisplaced proves the accounting
// does not cry wolf: a record whose attributes do not collide with the
// ingester's own reports nothing, even though the ingester still writes
// several of them.
func TestUncontestedAttributesAreNotReportedAsDisplaced(t *testing.T) {
	t.Parallel()
	logger, logs := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	record := &logspb.LogRecord{
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "line"}},
		SeverityText:   "ERROR",
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		TimeUnixNano:   uint64(time.Unix(1700000000, 0).UnixNano()),
		Attributes:     []*commonpb.KeyValue{stringKV("request_id", "abc123")},
	}

	msg := ing.logRecordToMessage(record, nil, nil, time.Now())

	if msg.Attrs["request_id"] != "abc123" {
		t.Errorf("attribute lost: %v", msg.Attrs)
	}
	if logs.Contains("OTLP record attributes dropped") {
		t.Errorf("a conforming record was reported as lossy: %s", logs)
	}
}

// TestConformingAttributesAreUntouched proves the ceiling did not change
// what a normal collector's records look like, including resource and scope
// precedence and a multi-kilobyte stack trace.
func TestConformingAttributesAreUntouched(t *testing.T) {
	t.Parallel()
	logger, logs := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	resourceKVs := []*commonpb.KeyValue{stringKV("service.name", "api"), stringKV("host.name", "node-1")}
	scopeKVs := []*commonpb.KeyValue{stringKV("scope", "http")}
	stack := strings.Repeat("frame\n", 500)
	record := &logspb.LogRecord{
		Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "line"}},
		Attributes: []*commonpb.KeyValue{
			stringKV("exception.stacktrace", stack),
			stringKV("host.name", "node-2"), // record attrs win over resource
		},
	}

	msg := ing.logRecordToMessage(record, resourceKVs, scopeKVs, time.Now())

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
