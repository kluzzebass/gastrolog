//go:build !linux && !darwin

// Package diskreserve reserves physical disk blocks for a file without
// changing its logical size. On platforms without a block-reservation syscall
// this is a no-op: callers still work, they just have no ENOSPC immunity.
package diskreserve

import "os"

// Blocks is a no-op on platforms without a block-reservation syscall.
func Blocks(_ *os.File, _ int64) error {
	return nil
}
