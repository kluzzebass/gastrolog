//go:build darwin || linux

package glcb

import "golang.org/x/sys/unix"

// madviseSequential hints the kernel that data will be read once, front to
// back, and kicks off asynchronous readahead for the whole mapping. It moves
// the read I/O onto a P-releasing syscall so the subsequent sequential mmap
// accesses fault in as minor faults instead of cold major faults that pin
// scheduler Ps inside non-preemptible kernel fault handlers under disk
// saturation (gastrolog-1io54g).
//
// Best-effort and idempotent: advice is a hint, and failures (e.g. a range
// that has already been unmapped) are ignored — the scan still reads
// correctly, just without the warm. Both darwin and linux expose Madvise and
// the two advice constants with identical semantics for our use.
func madviseSequential(data []byte) {
	if len(data) == 0 {
		return
	}
	// MADV_SEQUENTIAL: expect front-to-back access. The kernel reads further
	// ahead and drops pages behind the cursor, so a full-chunk drain does not
	// thrash the page cache against live ingest on the same volume.
	_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
	// MADV_WILLNEED: begin pulling the range into the page cache now, off the
	// scan's critical path. Readahead is scheduled asynchronously; the syscall
	// returns before completion, which is exactly the point — the blocking I/O
	// happens in the kernel readahead path, not in an mmap fault holding a P.
	_ = unix.Madvise(data, unix.MADV_WILLNEED)
}
