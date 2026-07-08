//go:build darwin

package raftwal

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate reserves size bytes of physical blocks for f without changing
// its logical size, so appends within the reservation can never fail with
// ENOSPC while replay's EOF-is-end-of-data invariant is untouched.
//
// F_PEOFPOSMODE allocates relative to the file's PHYSICAL end: calling this
// twice on the same file reserves twice the space. Callers must preallocate
// each segment exactly once (the WAL tracks its reserved spare for this).
func preallocate(f *os.File, size int64) error {
	store := unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}
	return unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &store)
}
