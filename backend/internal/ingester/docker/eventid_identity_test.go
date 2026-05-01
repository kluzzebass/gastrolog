package docker

import (
	"testing"
	"time"

	"gastrolog/internal/ingester/identitytest"
)

// TestEventIDIdentity pins gastrolog-44b9r for the Docker ingester.
// Docker requires a real daemon; we drive buildMessage directly —
// the seam where IngesterID and IngestTS land on the IngestMessage.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	const ingesterID = "test-docker-ingester"
	msg := buildMessage(map[string]string{"container": "probe"}, []byte("identity probe"), ingesterID, time.Now())
	identitytest.AssertHasIdentity(t, msg, ingesterID)
}
