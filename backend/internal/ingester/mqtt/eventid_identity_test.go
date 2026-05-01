package mqtt

import (
	"testing"
	"time"

	"gastrolog/internal/ingester/identitytest"
)

// TestEventIDIdentity pins gastrolog-44b9r for the MQTT ingester.
// MQTT requires a real broker, which we can't run in unit tests, so
// we drive buildMessage directly — the single seam where IngesterID
// and IngestTS land on the IngestMessage. Both v3 and v5 handlers
// funnel through this helper, so a regression at either site is
// caught here without needing a broker.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	const ingesterID = "test-mqtt-ingester"
	msg := buildMessage("topic/probe", 1, false, 7, []byte("identity probe"), ingesterID, time.Now())
	identitytest.AssertHasIdentity(t, msg, ingesterID)
}
