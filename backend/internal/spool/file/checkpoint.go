package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gastrolog/internal/spool"
)

const checkpointFileName = "replica_checkpoint"

var checkpointMagic = [8]byte{'G', 'L', 'O', 'G', 'S', 'P', 'L', 'C'}

const (
	checkpointVersion = 1
	checkpointSize    = 8 + 4 + 8 + 8 + 8 // magic + version + 3x uint64
)

// LoadReplicaCheckpoint reads durable M_r, C_r, and reclaim watermark from disk.
func (m *Manager) LoadReplicaCheckpoint() (spool.ReplicaCheckpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadReplicaCheckpointLocked()
}

func (m *Manager) loadReplicaCheckpointLocked() (spool.ReplicaCheckpoint, error) {
	path := filepath.Join(m.cfg.Dir, checkpointFileName)
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return spool.ReplicaCheckpoint{}, nil
		}
		return spool.ReplicaCheckpoint{}, err
	}
	if len(data) < checkpointSize {
		return spool.ReplicaCheckpoint{}, spool.ErrCheckpointCorrupt
	}
	if [8]byte(data[:8]) != checkpointMagic {
		return spool.ReplicaCheckpoint{}, spool.ErrCheckpointCorrupt
	}
	if ver := binary.LittleEndian.Uint32(data[8:12]); ver != checkpointVersion {
		return spool.ReplicaCheckpoint{}, spool.ErrCheckpointCorrupt
	}
	return spool.ReplicaCheckpoint{
		MaterializationH:  binary.LittleEndian.Uint64(data[12:20]),
		ConvergenceH:      binary.LittleEndian.Uint64(data[20:28]),
		ReclaimThroughSeq: binary.LittleEndian.Uint64(data[28:36]),
	}, nil
}

// SaveReplicaCheckpoint atomically persists replica watermarks.
func (m *Manager) SaveReplicaCheckpoint(ckpt spool.ReplicaCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prev, err := m.loadReplicaCheckpointLocked()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		if !errors.Is(err, spool.ErrCheckpointCorrupt) {
			return err
		}
		prev = spool.ReplicaCheckpoint{}
	}
	merged := prev.MergeMonotonic(ckpt)
	if merged.ReclaimThroughSeq > m.reclaimThroughSeq {
		m.reclaimThroughSeq = merged.ReclaimThroughSeq
	}

	buf := make([]byte, checkpointSize)
	copy(buf[:8], checkpointMagic[:])
	binary.LittleEndian.PutUint32(buf[8:12], checkpointVersion)
	binary.LittleEndian.PutUint64(buf[12:20], merged.MaterializationH)
	binary.LittleEndian.PutUint64(buf[20:28], merged.ConvergenceH)
	binary.LittleEndian.PutUint64(buf[28:36], merged.ReclaimThroughSeq)

	tmp := filepath.Join(m.cfg.Dir, checkpointFileName+".tmp")
	f, err := os.OpenFile(filepath.Clean(tmp), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, m.cfg.FileMode)
	if err != nil {
		return fmt.Errorf("spool: open checkpoint tmp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: write checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: sync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("spool: close checkpoint tmp: %w", err)
	}
	final := filepath.Join(m.cfg.Dir, checkpointFileName)
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("spool: rename checkpoint: %w", err)
	}
	if dir, err := os.Open(m.cfg.Dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// encodeCheckpoint is used by tests to inject corrupt checkpoints.
func encodeCheckpoint(ckpt spool.ReplicaCheckpoint) []byte {
	buf := make([]byte, checkpointSize)
	copy(buf[:8], checkpointMagic[:])
	binary.LittleEndian.PutUint32(buf[8:12], checkpointVersion)
	binary.LittleEndian.PutUint64(buf[12:20], ckpt.MaterializationH)
	binary.LittleEndian.PutUint64(buf[20:28], ckpt.ConvergenceH)
	binary.LittleEndian.PutUint64(buf[28:36], ckpt.ReclaimThroughSeq)
	return buf
}

func writeCheckpointFile(dir string, mode os.FileMode, ckpt spool.ReplicaCheckpoint) error {
	path := filepath.Join(dir, checkpointFileName)
	return os.WriteFile(path, encodeCheckpoint(ckpt), mode)
}
