package file

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/spool"
)

func TestReplicaCheckpointSurvivesManagerRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m1, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ckpt := spool.ReplicaCheckpoint{
		MaterializationH:  42,
		ConvergenceH:      40,
		ReclaimThroughSeq: 38,
	}
	if err := m1.SaveReplicaCheckpoint(ckpt); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })

	got, err := m2.LoadReplicaCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if got != ckpt {
		t.Fatalf("checkpoint = %+v, want %+v", got, ckpt)
	}
	if gotReclaim := m2.ReclaimThroughSeq(); gotReclaim != 38 {
		t.Fatalf("reclaim watermark = %d, want 38", gotReclaim)
	}
}

func TestReplicaCheckpointMergeMonotonicOnSave(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	if err := m.SaveReplicaCheckpoint(spool.ReplicaCheckpoint{MaterializationH: 10, ConvergenceH: 8, ReclaimThroughSeq: 7}); err != nil {
		t.Fatal(err)
	}
	if err := m.SaveReplicaCheckpoint(spool.ReplicaCheckpoint{MaterializationH: 5, ConvergenceH: 9, ReclaimThroughSeq: 6}); err != nil {
		t.Fatal(err)
	}
	got, err := m.LoadReplicaCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	want := spool.ReplicaCheckpoint{MaterializationH: 10, ConvergenceH: 9, ReclaimThroughSeq: 7}
	if got != want {
		t.Fatalf("checkpoint = %+v, want %+v", got, want)
	}
}

func TestReplicaCheckpointCorruptFileIgnoredOnLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, checkpointFileName)
	if err := os.WriteFile(path, []byte("garbage"), 0o640); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	_, err = m.LoadReplicaCheckpoint()
	if !errors.Is(err, spool.ErrCheckpointCorrupt) {
		t.Fatalf("LoadReplicaCheckpoint err = %v, want %v", err, spool.ErrCheckpointCorrupt)
	}
	if got := m.ReclaimThroughSeq(); got != 0 {
		t.Fatalf("reclaim watermark = %d, want 0 on corrupt checkpoint", got)
	}
}
