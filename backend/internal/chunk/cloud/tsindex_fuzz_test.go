package cloud

import "testing"

func FuzzFindStartPosition(f *testing.F) {
	// Seed corpus: empty, single entry, multiple entries, partial entry.
	f.Add([]byte{}, int64(0))
	f.Add(make([]byte, 12), int64(0))                            // one zero entry
	f.Add(make([]byte, 24), int64(1000))                         // two entries
	f.Add(make([]byte, 7), int64(0))                             // less than one entry
	f.Add(make([]byte, 15), int64(500))                          // one full + partial
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}, int64(0)) // ts=1<<56, pos=0

	f.Fuzz(func(t *testing.T, data []byte, tsNano int64) {
		// Must never panic.
		_, _ = FindStartPosition(data, tsNano)
	})
}
