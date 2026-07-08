//go:build !linux && !darwin

package raftwal

import "os"

// preallocate is a no-op on platforms without a block-reservation syscall.
// The WAL still works; it just has no ENOSPC immunity there.
func preallocate(_ *os.File, _ int64) error {
	return nil
}
