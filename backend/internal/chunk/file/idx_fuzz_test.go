package file

import "testing"

func FuzzDecodeIdxEntry(f *testing.F) {
	// Seed corpus: all zeros, all 0xff, realistic-looking entry.
	f.Add(make([]byte, IdxEntrySize))

	allOnes := make([]byte, IdxEntrySize)
	for i := range allOnes {
		allOnes[i] = 0xff
	}
	f.Add(allOnes)

	// A slightly varied pattern.
	varied := make([]byte, IdxEntrySize)
	for i := range varied {
		varied[i] = byte(i)
	}
	f.Add(varied)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < IdxEntrySize {
			return // DecodeIdxEntry requires exactly IdxEntrySize bytes
		}
		// Must never panic. Truncate to IdxEntrySize to match the contract.
		_ = DecodeIdxEntry(data[:IdxEntrySize])
	})
}

func FuzzDecodeIdxEntryRoundTrip(f *testing.F) {
	f.Add(make([]byte, IdxEntrySize))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < IdxEntrySize {
			return
		}
		entry := DecodeIdxEntry(data[:IdxEntrySize])

		// Re-encode and verify determinism.
		buf := make([]byte, IdxEntrySize)
		EncodeIdxEntry(entry, buf)

		entry2 := DecodeIdxEntry(buf)
		if entry.RawOffset != entry2.RawOffset ||
			entry.RawSize != entry2.RawSize ||
			entry.AttrOffset != entry2.AttrOffset ||
			entry.AttrSize != entry2.AttrSize ||
			entry.IngestSeq != entry2.IngestSeq ||
			entry.IngesterID != entry2.IngesterID {
			t.Fatal("round-trip mismatch on fixed-size fields")
		}
	})
}
