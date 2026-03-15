package cloud

import "encoding/binary"

// FindStartPosition binary-searches a TS index byte slice for the first
// entry with TS >= tsNano. The data is raw sorted entries: [i64 ts][u32 pos] × N.
// Returns (recordPos, true) if found, (0, false) if tsNano is after all entries
// or data is empty.
func FindStartPosition(data []byte, tsNano int64) (uint64, bool) {
	n := len(data) / tsIndexEntrySize
	if n == 0 {
		return 0, false
	}

	readTS := func(i int) int64 {
		off := i * tsIndexEntrySize
		return int64(binary.LittleEndian.Uint64(data[off:])) //nolint:gosec // G115: nanosecond timestamps fit in int64
	}
	readPos := func(i int) uint32 {
		off := i*tsIndexEntrySize + 8
		return binary.LittleEndian.Uint32(data[off:])
	}

	// Quick bounds check.
	if tsNano > readTS(n-1) {
		return 0, false // past all entries
	}
	if tsNano <= readTS(0) {
		return uint64(readPos(0)), true
	}

	// Binary search: first index i where TS[i] >= tsNano.
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if readTS(mid) < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return uint64(readPos(lo)), true
}
