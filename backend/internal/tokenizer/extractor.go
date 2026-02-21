package tokenizer

// KVExtractor tries to extract key=value pairs from a log message.
// Returns nil if the message is not in the expected format.
// Extractors should be conservative: return nil rather than emit false positives.
type KVExtractor func(msg []byte) []KeyValue

// CombinedExtract runs all extractors and merges results, deduplicating
// (key, value) pairs across extractors.
func CombinedExtract(msg []byte, extractors []KVExtractor) []KeyValue {
	var result []KeyValue
	var seen map[string]struct{}

	for _, ext := range extractors {
		pairs := ext(msg)
		if len(pairs) == 0 {
			continue
		}

		// Lazy init seen map only if we have results from multiple extractors.
		if seen == nil && result != nil {
			seen = make(map[string]struct{}, len(result)+len(pairs))
			for _, kv := range result {
				seen[kv.Key+"\x00"+kv.Value] = struct{}{}
			}
		}

		if seen == nil {
			// First extractor with results — take all pairs directly.
			result = pairs
			continue
		}

		// Subsequent extractors — deduplicate.
		for _, kv := range pairs {
			key := kv.Key + "\x00" + kv.Value
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				result = append(result, kv)
			}
		}
	}

	return result
}

// DefaultExtractors returns the standard set of KV extractors:
// heuristic KV, logfmt, and access log.
// JSON extraction is handled by the structural JSON index (WalkJSON),
// not the flat KV pipeline.
func DefaultExtractors() []KVExtractor {
	return []KVExtractor{
		ExtractKeyValues,
		ExtractLogfmt,
		ExtractAccessLog,
	}
}
