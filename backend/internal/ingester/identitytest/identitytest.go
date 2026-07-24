// Package identitytest provides shared assertions for the per-ingester
// EventID-identity tests filed under gastrolog-44b9r. Every ingester
// must produce IngesterMessages whose IngesterID matches the configured
// ID and whose IngestTS is non-zero — those are the two identity fields
// the ingestion pipeline needs to assemble a complete EventID. The test
// discipline catches regressions where a future ingester (or refactor)
// silently drops one of them.
package identitytest

import (
	"testing"

	"gastrolog/internal/pipeline/ingestion"
)

// AssertHasIdentity fails the test if msg lacks either identity field the
// ingestion pipeline needs to stamp a complete EventID downstream:
//   - IngesterID must equal the configured ID (non-empty + correct).
//   - IngestTS must be non-zero (the receive timestamp).
//
// Other EventID components (NodeID, IngestSeq) are stamped by the ingestion
// pipeline universally — those are tested at that layer.
func AssertHasIdentity(t *testing.T, msg ingestion.IngesterMessage, wantIngesterID string) {
	t.Helper()
	if msg.IngesterID == "" {
		t.Error("IngesterMessage.IngesterID is empty — ingester must set the configured ID on every emitted message (gastrolog-44b9r)")
	} else if msg.IngesterID != wantIngesterID {
		t.Errorf("IngesterMessage.IngesterID = %q, want %q (config ID must round-trip to the message)", msg.IngesterID, wantIngesterID)
	}
	if msg.IngestTS.IsZero() {
		t.Error("IngesterMessage.IngestTS is zero — ingester must stamp the receive time on every emitted message (gastrolog-44b9r)")
	}
}
