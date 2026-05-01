package docker

import (
	"time"

	"gastrolog/internal/orchestrator"
)

// buildMessage assembles an IngestMessage from a Docker log entry's
// extracted fields. Docker is daemon-only so we can't drive an
// end-to-end test in unit tests; centralising the construction here
// lets gastrolog-44b9r pin the IngesterID + IngestTS invariant
// directly via a unit test.
func buildMessage(attrs map[string]string, raw []byte, ingesterID string, now time.Time) orchestrator.IngestMessage {
	return orchestrator.IngestMessage{
		Attrs:      attrs,
		Raw:        raw,
		IngestTS:   now,
		IngesterID: ingesterID,
	}
}
