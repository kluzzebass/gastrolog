//go:build darwin

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

// Blocks reserves size bytes of physical blocks for f without changing its
// logical size, so writes within the reservation cannot ENOSPC.
//
// F_PEOFPOSMODE allocates relative to the file's PHYSICAL end: calling this
// twice on the same file reserves twice the space. Callers must reserve each
// file exactly once.
func Blocks(f *os.File, size int64) error {
	store := unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}
	return unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &store)
}
