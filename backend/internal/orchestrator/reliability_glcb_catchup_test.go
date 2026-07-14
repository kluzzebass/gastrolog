package orchestrator_test

import (
	"bytes"
	"gastrolog/internal/chunk"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOrchPipeline_GLCBReplicaCatchup reproduces the lost-replica incident:
// a home holds the FSM manifest entry for a sealed chunk but not its GLCB
// bytes (it missed the build window; source segments are long released, so
// the record-stream catch-up path pipeline vaults deliberately skip cannot
// help). The vault catch-up sweep must pull the GLCB from a peer home,
// verify it, and promote it — byte-identical to the peers' copies — with no
// operator action.
func TestOrchPipeline_GLCBReplicaCatchup(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	// One sealed chunk, GLCB present on every home.
	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords, "glcb-catchup")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk, got %d", len(entries))
	}
	e := entries[0]
	homeIdxs := []int{0, 1, 2}
	h.waitGLCBsOnHomes(v, homeIdxs, entries)

	// A follower home loses its copy. The sealed manifest's pending build
	// state is already cleared cluster-wide, so no build path will ever
	// recreate this file — before the catch-up pull existed, this state
	// starved retention forever while the FSM still counted the home as a
	// holder.
	victim := h.nodeIDs[1]
	victimPath := h.pipelineGLCBPath(victim, v, e.ID)
	want, err := os.ReadFile(h.pipelineGLCBPath(h.nodeIDs[0], v, e.ID))
	if err != nil {
		t.Fatalf("read reference GLCB on leader home: %v", err)
	}
	if err := os.Remove(victimPath); err != nil {
		t.Fatalf("remove victim GLCB: %v", err)
	}

	// The vault catch-up sweep cron (13/33/53s) stat-misses the GLCB,
	// pulls it from a peer home, verifies seal metadata against the FSM
	// entry, and renames it into place.
	deadline := time.Now().Add(orchHarnessConvWait)
	recovered := false
	for time.Now().Before(deadline) {
		got, err := os.ReadFile(victimPath)
		if err == nil {
			if !bytes.Equal(got, want) {
				t.Fatalf("recovered GLCB differs: %d bytes, want %d", len(got), len(want))
			}
			recovered = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !recovered {
		t.Fatal("GLCB not recovered from peer within convergence window")
	}

	// No staging leftovers next to the promoted blob.
	dirEntries, err := os.ReadDir(filepath.Dir(victimPath))
	if err != nil {
		t.Fatalf("read chunk dir: %v", err)
	}
	for _, de := range dirEntries {
		if strings.HasPrefix(de.Name(), ".glcb.pull.") {
			t.Fatalf("staging temp file left behind: %s", de.Name())
		}
	}

	// Peer homes keep their original copies untouched.
	for _, idx := range []int{0, 2} {
		got, err := os.ReadFile(h.pipelineGLCBPath(h.nodeIDs[idx], v, e.ID))
		if err != nil {
			t.Fatalf("peer home %d GLCB unreadable after catch-up: %v", idx, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("peer home %d GLCB changed during catch-up", idx)
		}
	}

	// Holder receipts re-earn: with the bytes recovered, the victim's
	// residency claim must return within a sweep — replica counts report
	// bytes truth end to end (revoke on loss is asserted separately; here
	// the pull may complete before the revoke sweep ever observes the gap).
	h.waitChunkHolders(v, e.ID, homeIdxs)
}

// waitChunkHolders waits until the FSM's holder receipts for chunkID
// exactly match the given home indexes.
func (h *orchRelHarness) waitChunkHolders(v vaultSpec, chunkID chunk.ChunkID, homeIdxs []int) {
	h.t.Helper()
	want := make(map[string]bool, len(homeIdxs))
	for _, idx := range homeIdxs {
		want[h.nodeIDs[idx]] = true
	}
	leader := h.nodes[h.nodeIDs[homeIdxs[0]]]
	deadline := time.Now().Add(orchHarnessConvWait)
	var got []string
	for time.Now().Before(deadline) {
		got = leader.orch.ChunkResidency(v.id, chunkID)
		if len(got) == len(want) {
			all := true
			for _, n := range got {
				if !want[n] {
					all = false
					break
				}
			}
			if all {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	h.t.Fatalf("holder receipts never converged: got %v, want %d homes", got, len(want))
}

// TestOrchPipeline_ChunkHolderRevokeOnByteLoss pins the accounting half of
// the lost-replica incident: a home that loses its GLCB bytes must stop
// being counted as a holder. The FSM once reported the full placement set
// as residency for every live sealed chunk — x4 replicas claimed while a
// wedged node held 1 of ~300 — so the count was an assumption, not truth.
// Here the victim's pull sources are gone (all peers lose the chunk too),
// so recovery cannot mask the revocation.
func TestOrchPipeline_ChunkHolderRevokeOnByteLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	homeIdxs := []int{0, 1, 2}

	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords, "holder-revoke")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk, got %d", len(entries))
	}
	e := entries[0]
	h.waitGLCBsOnHomes(v, homeIdxs, entries)

	// All three homes earn receipts.
	h.waitChunkHolders(v, e.ID, homeIdxs)

	// Every copy vanishes (disk swap / operator deletion on all homes).
	// No pull can succeed; the only correct outcome is honest accounting.
	for _, idx := range homeIdxs {
		if err := os.Remove(h.pipelineGLCBPath(h.nodeIDs[idx], v, e.ID)); err != nil {
			t.Fatalf("remove GLCB on home %d: %v", idx, err)
		}
	}

	// Receipts must drain to zero as each home's sweep revokes its claim.
	deadline := time.Now().Add(orchHarnessConvWait)
	var got []string
	for time.Now().Before(deadline) {
		got = h.nodes[h.nodeIDs[0]].orch.ChunkResidency(v.id, e.ID)
		if len(got) == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("holder receipts never revoked after byte loss on every home: still %v", got)
}
