package segment

import (
	"hash/crc32"
	"io"
	"sync"
)

const crcReadChunk = 256 << 10

// hashFeed feeds bytes [start:end) into h without loading the whole range at once.
// crcBufPool recycles feed buffers: pull-verify checksums every collected
// segment, and a fresh 256KB buffer per feed measured ~1GB/run of churn at
// parallel-pull rates.
var crcBufPool = sync.Pool{New: func() any { b := make([]byte, crcReadChunk); return &b }}

func hashFeed(h io.Writer, r io.ReaderAt, start, end uint32) error {
	if end <= start {
		return nil
	}
	bp := crcBufPool.Get().(*[]byte)
	defer crcBufPool.Put(bp)
	buf := *bp
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
	if err := hashFeed(h, r, start, end); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
