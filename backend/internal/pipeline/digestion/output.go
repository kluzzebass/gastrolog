package digestion

import "gastrolog/internal/record"

// Output is one digestion result. Record is non-nil on success; Err is set on
// parse/digest failure. Ack is passed through from the ingestion message.
type Output struct {
	Record *record.Record
	Err    error
	Ack    chan<- error
}
