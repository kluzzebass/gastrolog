package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"gastrolog/internal/ingester/identitytest"
)

// TestEventIDIdentity pins gastrolog-44b9r for the Kafka ingester.
// Kafka is broker-only (no embedded broker available in unit tests),
// so we test buildMessage directly — that's the single seam where
// IngesterID and IngestTS land on the IngestMessage. A regression
// here (e.g. a future refactor that drops one of the fields) is
// caught without needing a real broker.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	const ingesterID = "test-kafka-ingester"
	now := time.Now()
	rec := &kgo.Record{
		Topic:     "logs",
		Partition: 0,
		Offset:    42,
		Value:     []byte("identity probe"),
		Timestamp: now,
	}
	msg := buildMessage(rec, ingesterID, now)
	identitytest.AssertHasIdentity(t, msg, ingesterID)
}
