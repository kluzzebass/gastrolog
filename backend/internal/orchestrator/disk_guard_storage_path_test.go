package orchestrator

// Coverage for storage-path resolution: system.FileStorage paths are stored
// relative by the same convention vault "dir" params are (see
// resolveVaultDir's contract in reconfig_vaults.go) — the operator
// configures "storage/disk-1", and each node resolves it against its own
// home. Handing fs.Path RAW to the disk guard made
// statfs("storage/disk-1") resolve against the process's CWD instead of the
// node's home; it failed silently and worstFreeOf skipped the storage
// forever: no sample, no protect, an inherited "10%" threshold resolving
// against a 0 total ("0 B"). Guard fixtures that use absolute temp paths
// cannot catch that, hence this file.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// TestResolveStoragePath pins the resolution contract itself: relative
// paths join against vaultsDir, absolute paths and an unknown (empty)
// vaultsDir pass through unchanged. Pure string manipulation — inherently
// independent of the process's working directory, unlike a test that
// exercises resolution only by way of happening to run from the right CWD.
func TestResolveStoragePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		vaultsDir string
		want      string
	}{
		{"relative path joins vaultsDir", "storage/disk-1", "/home/node1", "/home/node1/storage/disk-1"},
		{"absolute path passes through even with vaultsDir set", "/mnt/disk1", "/home/node1", "/mnt/disk1"},
		{"empty vaultsDir passes the relative path through unchanged", "storage/disk-1", "", "storage/disk-1"},
		{"empty path stays empty", "", "/home/node1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveStoragePath(tt.path, tt.vaultsDir); got != tt.want {
				t.Fatalf("resolveStoragePath(%q, %q) = %q, want %q", tt.path, tt.vaultsDir, got, tt.want)
			}
		})
	}
}

// TestRefreshStorageGuardsResolvesRelativePathAgainstVaultsDir is the
// end-to-end pin: a NodeStorageConfig with a RELATIVE FileStorage.Path (the
// operator-facing convention, "storage/disk-1") plus a real vaultsDir base
// must produce a REAL, successful statfs sample — not "no sample" — through
// the actual production path (refreshVaultDiskGuards -> refreshStorageGuards
// -> evaluateStorages), using the guard's real statfsSample, not an
// injected fake. The test's own working directory is deliberately moved
// somewhere else first (t.Chdir to an unrelated empty temp dir) to prove
// the resolved path does NOT depend on the process's CWD — reproducing the
// exact failure mode this pins (statfs("storage/disk-1") silently resolving
// against CWD instead of the node home).
func TestRefreshStorageGuardsResolvesRelativePathAgainstVaultsDir(t *testing.T) {
	home := t.TempDir()
	const relPath = "storage/disk-1"
	if err := os.MkdirAll(filepath.Join(home, relPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Move the process CWD somewhere with NOTHING at "storage/disk-1" —
	// if resolution ever falls back to CWD-relative (the bug), the real
	// statfs call fails here instead of accidentally succeeding.
	elsewhere := t.TempDir()
	t.Chdir(elsewhere)

	vaultID := glid.New()
	storageID := glid.New()
	const nodeID = "node-1"

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "on-disk",
			Enabled: true,
			Type:    system.VaultTypeFile,
		}},
	}
	rt := system.Runtime{
		VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {
				{StorageID: storageID.String(), Leader: true},
			},
		},
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []system.FileStorage{{
				ID:   storageID,
				Name: "disk-1",
				Path: relPath,
			}},
		}},
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	orch.vaultsDir = home
	orch.setSystemLoader(testSystemLoaderWithRuntime{cfg: cfg, rt: rt})
	// Deliberately NOT overriding orch.diskGuard.sample: this must exercise
	// the real statfs syscall against the real resolved directory.

	orch.refreshVaultDiskGuards(context.Background())
	orch.diskGuard.evaluateStorages(orch.alerts)

	g := orch.diskGuard
	g.mu.Lock()
	s := g.storages[storageID.String()]
	g.mu.Unlock()
	if s == nil {
		t.Fatal("storage guard entry was not registered")
	}
	if s.path != filepath.Join(home, relPath) {
		t.Fatalf("registered guard path = %q, want the resolved absolute path %q", s.path, filepath.Join(home, relPath))
	}
	if s.lastFree.Load() == 0 {
		t.Fatal("statfs must have produced a real, non-zero free-space sample — got none, the exact live-cluster symptom (\"no sample\" / 0 B)")
	}
	if s.lastFloor.Load() == 0 {
		t.Fatal("the inherited floor must resolve against the REAL volume total, not a 0 total from a failed sample")
	}
}

// TestRefreshStorageGuardsAbsolutePathUnaffected pins that the fix is a
// no-op for absolute paths — every existing guard fixture (disk_guard_test.go,
// disk_guard_storage_discovery_test.go) uses paths like "volA"/"storA" as
// opaque fake-sampler keys with vaultsDir explicitly disabled, so this adds
// the one case those don't cover: a REAL absolute path with vaultsDir SET,
// proving resolveStoragePath's absolute-path passthrough holds even when a
// base is available to (wrongly) join against.
func TestRefreshStorageGuardsAbsolutePathUnaffected(t *testing.T) {
	absDir := t.TempDir()

	vaultID := glid.New()
	storageID := glid.New()
	const nodeID = "node-1"

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "on-disk",
			Enabled: true,
			Type:    system.VaultTypeFile,
		}},
	}
	rt := system.Runtime{
		VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {
				{StorageID: storageID.String(), Leader: true},
			},
		},
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []system.FileStorage{{
				ID:   storageID,
				Name: "disk-1",
				Path: absDir, // already absolute
			}},
		}},
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	orch.vaultsDir = t.TempDir() // a DIFFERENT dir — must not get joined in
	orch.setSystemLoader(testSystemLoaderWithRuntime{cfg: cfg, rt: rt})

	orch.refreshVaultDiskGuards(context.Background())

	g := orch.diskGuard
	g.mu.Lock()
	s := g.storages[storageID.String()]
	g.mu.Unlock()
	if s == nil {
		t.Fatal("storage guard entry was not registered")
	}
	if s.path != absDir {
		t.Fatalf("absolute path must pass through unchanged, got %q, want %q", s.path, absDir)
	}
}
