//go:build linux

// Package diskreserve reserves physical disk blocks for a file without
// changing its logical size, so a later write into the reserved range cannot
// fail with ENOSPC on a full volume. Used by the raft WAL (term/log writes
// must never fail — gastrolog-67gvjo) and the crash-forensics log (a panic on
// a full volume must still record its traceback).
package diskreserve

import (
	"os"

	"golang.org/x/sys/unix"
)

// Blocks reserves size bytes of physical blocks for f WITHOUT changing its
// logical size (FALLOC_FL_KEEP_SIZE): appends/writes within the reservation
// cannot ENOSPC, while any EOF-is-end-of-data reader is unaffected. Reserving
// an already-reserved range is a no-op.
func Blocks(f *os.File, size int64) error {
	return unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size)
}
