package orchestrator_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestOrchPipeline_StorageClassChoosesPipelineVolume runs two vaults of
// different storage classes through the full pipeline on a 4-node cluster:
// vault A (class 1) homed on every node, vault B (class 2) homed on three.
// Every node has one storage of each class, in its own directory. Each home's
// segments and GLCBs for a vault land on the storage of that vault's class;
// the node that only originates segments for B keeps them under its home, and
// nothing of either vault lands on a storage its class did not choose. A
// restarted home serves its records from the same storage.
func TestOrchPipeline_StorageClassChoosesPipelineVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withStorageClassVault(2, []int{0, 1, 2}),
		withMatchAllRoute(0, 1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	a, b := h.vaults[0], h.vaults[1]
	allIdxs := []int{0, 1, 2, 3}
	bHomeIdxs := []int{0, 1, 2}
	originOnly := h.nodes[h.nodeIDs[3]] // originates segments for B, homes A

	const total = 2 * pipelineChunkMaxRecords
	bodies := h.submitIngestRecords(originOnly.id, total, "storage-class")

	aEntries := h.waitSealedRecords(a, h.nodeIDs[0], total)
	bEntries := h.waitSealedRecords(b, h.nodeIDs[0], total)
	h.waitGLCBsOnHomes(a, allIdxs, aEntries)
	h.waitGLCBsOnHomes(b, bHomeIdxs, bEntries)

	for _, idx := range allIdxs {
		n := h.nodes[h.nodeIDs[idx]]
		classOne := filepath.Join(n.home, "chunks")
		classTwo := classStoragePath(n, 2)
		assertGLCBsUnder(t, n.label+" vault A", filepath.Join(classOne, "segments", a.id.String(), "chunks"), aEntries)
		assertNoPipelineRoot(t, n.label+" vault A on the class-2 storage", filepath.Join(classTwo, "segments", a.id.String()))
		assertNoPipelineRoot(t, n.label+" vault B on the class-1 storage", filepath.Join(classOne, "segments", b.id.String()))
		if n == originOnly {
			assertNoPipelineRoot(t, n.label+" vault B on a storage it does not home", filepath.Join(classTwo, "segments", b.id.String()))
			assertNoGLCBs(t, n.label+" vault B origin root", filepath.Join(n.home, "segments", b.id.String()))
			continue
		}
		assertGLCBsUnder(t, n.label+" vault B", filepath.Join(classTwo, "segments", b.id.String(), "chunks"), bEntries)
		assertNoGLCBs(t, n.label+" vault B under the node home", filepath.Join(n.home, "segments", b.id.String()))
	}
	if _, err := os.Stat(filepath.Join(originOnly.home, "segments", b.id.String())); err != nil {
		t.Fatalf("origin-only node has no pipeline root for vault B under its home: %v", err)
	}

	follower := h.nodeIDs[1]
	h.waitSearchable(b, follower, total)
	h.assertSearchBodiesExactly(b, follower, bodies)
	h.waitSearchable(a, originOnly.id, total)
	h.assertSearchBodiesExactly(a, originOnly.id, bodies)

	// Restart a home of both vaults: it serves both from the storages it
	// wrote them to.
	h.stopNode(follower)
	h.startNode(follower)
	h.waitForAllReady()
	n := h.nodes[follower]
	assertGLCBsUnder(t, n.label+" vault B after restart", filepath.Join(classStoragePath(n, 2), "segments", b.id.String(), "chunks"), bEntries)
	h.waitSearchable(b, follower, total)
	h.assertSearchBodiesExactly(b, follower, bodies)
	h.waitSearchable(a, follower, total)
	h.assertSearchBodiesExactly(a, follower, bodies)
}

func assertGLCBsUnder(t *testing.T, what, chunkRoot string, entries []vaultctlfsm.ManifestEntry) {
	t.Helper()
	for _, e := range entries {
		path := filepath.Join(chunkRoot, e.ID.String(), "data.glcb")
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s: GLCB for chunk %s not at %s: %v", what, e.ID, path, err)
		}
	}
}

func assertNoPipelineRoot(t *testing.T, what, root string) {
	t.Helper()
	if _, err := os.Stat(root); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("%s: pipeline root %s exists (err=%v)", what, root, err)
	}
}

func assertNoGLCBs(t *testing.T, what, root string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "chunks"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("%s: %v", what, err)
	}
	var ids []chunk.ChunkID
	for _, e := range entries {
		if id, err := chunk.ParseChunkID(e.Name()); err == nil {
			ids = append(ids, id)
		}
	}
	if len(ids) > 0 {
		t.Fatalf("%s: GLCBs under %s: %v", what, root, ids)
	}
}
