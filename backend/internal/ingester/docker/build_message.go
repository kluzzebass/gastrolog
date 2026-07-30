package docker

import (
	"gastrolog/internal/pipeline/ingestion"
	"time"
)

// buildMessage assembles an IngestMessage from a Docker log entry's
// extracted fields. Docker is daemon-only so we can't drive an
// end-to-end test in unit tests; centralising the construction here
// lets a unit test pin the IngesterID + IngestTS invariant directly.
func buildMessage(attrs map[string]string, raw []byte, ingesterID string, now time.Time) ingestion.IngesterMessage {
	return ingestion.IngesterMessage{
		Attrs:      attrs,
		Raw:        raw,
		IngestTS:   now,
		IngesterID: ingesterID,
	}
}
