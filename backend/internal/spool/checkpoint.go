package spool

import "errors"

// ErrCheckpointCorrupt is returned when a replica checkpoint file is unreadable.
var ErrCheckpointCorrupt = errors.New("spool: corrupt replica checkpoint")

// ReplicaCheckpoint holds durable per-replica watermarks for the sequenced write path.
type ReplicaCheckpoint struct {
	MaterializationH  uint64 // M_r
	ConvergenceH      uint64 // C_r
	ReclaimThroughSeq uint64
}

// MergeMonotonic returns ckpt with each field set to the max of a and b.
func (a ReplicaCheckpoint) MergeMonotonic(b ReplicaCheckpoint) ReplicaCheckpoint {
	out := a
	if b.MaterializationH > out.MaterializationH {
		out.MaterializationH = b.MaterializationH
	}
	if b.ConvergenceH > out.ConvergenceH {
		out.ConvergenceH = b.ConvergenceH
	}
	if b.ReclaimThroughSeq > out.ReclaimThroughSeq {
		out.ReclaimThroughSeq = b.ReclaimThroughSeq
	}
	return out
}

// CheckpointPersistence loads and saves replica watermarks alongside spool storage.
type CheckpointPersistence interface {
	LoadReplicaCheckpoint() (ReplicaCheckpoint, error)
	SaveReplicaCheckpoint(ReplicaCheckpoint) error
}

// ReclaimWatermarkReader reports the spool reclaim safety watermark.
type ReclaimWatermarkReader interface {
	ReclaimThroughSeq() uint64
}

// ReclaimWatermarkSetter updates the spool reclaim safety watermark.
type ReclaimWatermarkSetter interface {
	SetReclaimThroughSeq(seq uint64)
}
