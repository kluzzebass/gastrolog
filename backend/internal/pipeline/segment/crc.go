package segment

import (
	"hash"
	"hash/crc32"
	"io"
)

const crcReadChunk = 256 << 10

// crc32IEEEFeed checksums bytes [start:end) into h without loading the whole range at once.
func crc32IEEEFeed(h hash.Hash32, r io.ReaderAt, start, end uint32) error {
	if end <= start {
		return nil
	}
	buf := make([]byte, crcReadChunk)
	pos := int64(start)
	remain := int64(end - start)
	for remain > 0 {
		toRead := min(int64(len(buf)), remain)
		n, err := r.ReadAt(buf[:toRead], pos)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		if _, err := h.Write(buf[:n]); err != nil {
			return err
		}
		pos += int64(n)
		remain -= int64(n)
	}
	return nil
}

// crc32IEEEOver checksums bytes [start:end) without loading the whole range at once.
func crc32IEEEOver(r io.ReaderAt, start, end uint32) (uint32, error) {
	h := crc32.NewIEEE()
	if err := crc32IEEEFeed(h, r, start, end); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
