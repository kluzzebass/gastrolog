package spool

import "errors"

var (
	// ErrReclaimBlocked is returned when a segment still holds sequences needed
	// for materialization/reconcile or is the active writable segment.
	ErrReclaimBlocked = errors.New("spool: segment reclaim blocked by safety watermark")
	// ErrSegmentNotSealed is returned when reclaim is attempted on an unsealed segment.
	ErrSegmentNotSealed = errors.New("spool: cannot reclaim unsealed segment")
	// ErrSegmentNotFound is returned when a reclaim target does not exist.
	ErrSegmentNotFound = errors.New("spool: segment not found")
)

// Reclaimable reports whether a sealed segment may be deleted given the
// materialization safety watermark. reclaimThroughSeq is the highest vault_seq
// whose spool bytes are no longer required as a materialize/reconcile source.
// active is the manager's currently writable window; it is unsealed when set.
func Reclaimable(meta SegmentMeta, reclaimThroughSeq uint64, active WindowID) error {
	if active.Start != 0 && active.End != 0 && meta.Window == active {
		return ErrReclaimBlocked
	}
	if !meta.Sealed {
		return ErrSegmentNotSealed
	}
	if meta.EndSeq > 0 {
		if meta.EndSeq > reclaimThroughSeq {
			return ErrReclaimBlocked
		}
		return nil
	}
	if meta.LastSeq > reclaimThroughSeq {
		return ErrReclaimBlocked
	}
	return nil
}
