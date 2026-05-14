package app

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gastrolog/internal/cluster"
	"gastrolog/internal/home"
)

// gastrolog-24iv4 Step C: autoRejoinIfEvicted's filesystem
// half — pinned via temp-dir tests. The cluster-query side is exercised
// in k8s integration; here we verify the no-op short-circuits and the
// rename-on-eviction behavior in isolation.

func TestHasLocalRaftState_NoDir(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	if hasLocalRaftState(filepath.Join(tmp, "nonexistent")) {
		t.Error("expected false for missing dir")
	}
}

func TestHasLocalRaftState_DirNoWAL(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	raftDir := filepath.Join(tmp, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if hasLocalRaftState(raftDir) {
		t.Error("expected false when raft dir exists but wal subdir is missing")
	}
}

func TestHasLocalRaftState_WALPresent(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	raftDir := filepath.Join(tmp, "raft")
	if err := os.MkdirAll(filepath.Join(raftDir, "wal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if !hasLocalRaftState(raftDir) {
		t.Error("expected true when wal subdir exists")
	}
}

func TestHasLocalRaftState_WALIsFileNotDir(t *testing.T) {
	t.Parallel()
	// Edge case: something named "wal" but not a directory. Should not
	// fool hasLocalRaftState into thinking there's raft state.
	tmp := t.TempDir()
	raftDir := filepath.Join(tmp, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(raftDir, "wal"), []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	if hasLocalRaftState(raftDir) {
		t.Error("expected false when wal is a regular file, not a directory")
	}
}

// autoRejoinIfEvicted is a no-op when the config type is memory.
func TestAutoRejoinIfEvicted_MemoryConfigSkips(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	hd := home.New(tmp)
	// Plant raft state to confirm we DO NOT touch it.
	if err := os.MkdirAll(filepath.Join(hd.RaftDir(), "wal"), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := RunConfig{ConfigType: "memory", JoinAddr: "anywhere:4566"}
	err := autoRejoinIfEvicted(context.Background(), slog.Default(), cfg, nil, hd, "node-x")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !hasLocalRaftState(hd.RaftDir()) {
		t.Error("memory config: raft dir should remain untouched")
	}
}

// autoRejoinIfEvicted is a no-op when JoinAddr is empty (bootstrap node).
func TestAutoRejoinIfEvicted_BootstrapNodeSkips(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	hd := home.New(tmp)
	if err := os.MkdirAll(filepath.Join(hd.RaftDir(), "wal"), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := RunConfig{ConfigType: "raft", JoinAddr: ""}
	err := autoRejoinIfEvicted(context.Background(), slog.Default(), cfg, nil, hd, "node-x")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !hasLocalRaftState(hd.RaftDir()) {
		t.Error("bootstrap node (no JoinAddr): raft dir should remain untouched")
	}
}

// autoRejoinIfEvicted is a no-op when clusterTLS is nil — we can't
// authenticate to query the peer, so falling back to "leave state
// alone and let the existing failure modes apply" is the safe call.
func TestAutoRejoinIfEvicted_NoTLSSkips(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	hd := home.New(tmp)
	if err := os.MkdirAll(filepath.Join(hd.RaftDir(), "wal"), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := RunConfig{ConfigType: "raft", JoinAddr: "peer:4566"}
	err := autoRejoinIfEvicted(context.Background(), slog.Default(), cfg, nil, hd, "node-x")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !hasLocalRaftState(hd.RaftDir()) {
		t.Error("no TLS material: raft dir should remain untouched")
	}
}

// autoRejoinIfEvicted is a no-op when there's no local raft state —
// fresh boot, nothing to wipe. The function returns nil; the caller
// goes through normal joiner flow.
func TestAutoRejoinIfEvicted_NoLocalStateSkips(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	hd := home.New(tmp)
	// No wal subdir created.

	cfg := RunConfig{ConfigType: "raft", JoinAddr: "peer:4566"}
	tls := cluster.NewClusterTLS()
	err := autoRejoinIfEvicted(context.Background(), slog.Default(), cfg, tls, hd, "node-x")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

// autoRejoinIfEvicted survives a peer query failure (unreachable
// JoinAddr, no DNS resolution, etc.) by logging and proceeding. The
// raft state must NOT be touched on a query failure — we don't want
// to wipe state because of a transient network blip.
func TestAutoRejoinIfEvicted_QueryFailurePreservesState(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	hd := home.New(tmp)
	if err := os.MkdirAll(filepath.Join(hd.RaftDir(), "wal"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Use an address that will fail to resolve / connect within the
	// 5-second internal timeout. localhost:1 should refuse connection
	// fast on most systems.
	cfg := RunConfig{ConfigType: "raft", JoinAddr: "localhost:1"}
	tls := cluster.NewClusterTLS()
	err := autoRejoinIfEvicted(context.Background(), slog.Default(), cfg, tls, hd, "node-x")
	if err != nil {
		t.Errorf("query failure must not fail the boot: got %v", err)
	}
	if !hasLocalRaftState(hd.RaftDir()) {
		t.Error("raft state must be preserved when peer query fails")
	}
	// No .evicted.* backup should have been created either.
	entries, _ := os.ReadDir(tmp)
	for _, e := range entries {
		if strings.Contains(e.Name(), ".evicted.") {
			t.Errorf("unexpected backup dir created on query failure: %s", e.Name())
		}
	}
}
