package orchestrator

// Digester processes an IngestMessage in-place before it is stored.
// Digesters enrich messages by adding, modifying, or removing attributes
// based on message content. They must not modify Raw or timestamps.
//
// Digesters are best-effort: a parse failure simply means no enrichment
// is applied. Implementations must not return errors or panic.
type Digester interface {
	Digest(msg *IngestMessage)
}
