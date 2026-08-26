package kafka

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"gastrolog/internal/ingester/limits"
)

// TestHeaderFloodIsBounded proves a record carrying tens of thousands of
// headers cannot explode the attribute map — or, downstream, index
// cardinality. The record's value still lands: headers are dropped, the
// record is not.
func TestHeaderFloodIsBounded(t *testing.T) {
	t.Parallel()

	headers := make([]kgo.RecordHeader, 50_000)
	for i := range headers {
		headers[i] = kgo.RecordHeader{Key: "k" + strconv.Itoa(i), Value: []byte("v")}
	}
	rec := &kgo.Record{Topic: "logs", Value: []byte("the log line"), Headers: headers}

	msg, dropped := buildMessage(rec, "test-kafka", time.Now())

	fromProducer := 0
	for k := range msg.Attrs {
		if strings.HasPrefix(k, "k") {
			fromProducer++
		}
	}
	if fromProducer > limits.Records.Count {
		t.Errorf("stored %d producer headers, past the %d ceiling", fromProducer, limits.Records.Count)
	}
	if dropped == 0 {
		t.Error("headers were dropped without being reported")
	}
	if string(msg.Raw) != "the log line" {
		t.Errorf("the record's payload was lost: %q", msg.Raw)
	}
	if msg.Attrs["kafka_topic"] != "logs" {
		t.Errorf("the ingester's own attributes were crowded out: %v", msg.Attrs["kafka_topic"])
	}
}

// TestConformingHeadersAreUntouched proves the ceiling did not change what a
// normal producer's records look like.
func TestConformingHeadersAreUntouched(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{
		Topic: "logs",
		Value: []byte("line"),
		Headers: []kgo.RecordHeader{
			{Key: "trace_id", Value: []byte("abc123")},
			{Key: "source", Value: []byte("api")},
		},
	}

	msg, dropped := buildMessage(rec, "test-kafka", time.Now())

	if dropped != 0 {
		t.Errorf("dropped %d conforming headers", dropped)
	}
	if msg.Attrs["trace_id"] != "abc123" || msg.Attrs["source"] != "api" {
		t.Errorf("conforming headers altered: %v", msg.Attrs)
	}
}
