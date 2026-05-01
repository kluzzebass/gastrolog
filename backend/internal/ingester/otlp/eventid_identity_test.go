package otlp

import (
	"testing"
	"time"

	"gastrolog/internal/ingester/identitytest"
)

// TestEventIDIdentity pins gastrolog-44b9r for the OTLP ingester. The
// shared listenAndStartOTLP helper hard-codes the ingester ID to
// "test-otlp"; we round-trip an OTLP record and assert that ID
// arrives on the IngestMessage along with a non-zero IngestTS.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	httpAddr, _, out := listenAndStartOTLP(t, 4)

	req := makeExportRequest(nil, nil,
		makeStringLogRecord("identity probe", time.Now().Truncate(time.Microsecond)))
	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	identitytest.AssertHasIdentity(t, recv(t, out), "test-otlp")
}
