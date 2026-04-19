package file

import (
	"fmt"
	"os"
)

// ReadIdxLogEntries reads all idx.log record entries from path without opening
// a ChunkManager (no directory lock). Used for offline verification between
// replicas (e.g. leader vs follower homes).
func ReadIdxLogEntries(idxPath string) ([]IdxEntry, error) {
	fi, err := os.Stat(idxPath)
	if err != nil {
		return nil, err
	}
	sz := fi.Size()
	if sz < int64(IdxHeaderSize) {
		return nil, fmt.Errorf("idx.log too small: %d bytes", sz)
	}
	body := sz - int64(IdxHeaderSize)
	if body%int64(IdxEntrySize) != 0 {
		return nil, fmt.Errorf("idx.log size %d: body after header not multiple of entry size", sz)
	}
	n := RecordCount(sz)

	f, err := os.Open(idxPath) //nolint:gosec // G304: path supplied by operator tool
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	out := make([]IdxEntry, 0, n)
	var buf [IdxEntrySize]byte
	for i := range n {
		if _, err := f.ReadAt(buf[:], IdxFileOffset(i)); err != nil {
			return nil, fmt.Errorf("read idx entry %d: %w", i, err)
		}
		out = append(out, DecodeIdxEntry(buf[:]))
	}
	return out, nil
}
