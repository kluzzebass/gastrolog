//go:build linux

package raftwal

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate reserves size bytes of physical blocks for f WITHOUT changing
// its logical size (FALLOC_FL_KEEP_SIZE): appends within the reservation can
// never fail with ENOSPC, while replay's EOF-is-end-of-data invariant is
// untouched. Reserving an already-reserved range is a no-op.
func preallocate(f *os.File, size int64) error {
	return unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size)
}
