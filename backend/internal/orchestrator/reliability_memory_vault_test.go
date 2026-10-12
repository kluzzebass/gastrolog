package orchestrator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// vaultQuery scopes a search to v.
func vaultQuery(v vaultSpec) query.Query {
	return query.Query{BoolExpr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "vault_id", Value: v.id.String()}}
}

// servedCount runs v's match-all search on nodeID through the engine the
// query server uses and counts the records served.
func (h *orchRelHarness) servedCount(v vaultSpec, nodeID string) (int, error) {
	n := h.nodes[nodeID]
	if n == nil || n.orch == nil {
		return 0, fmt.Errorf("node %s not running", nodeID)
	}
	seq, _ := n.orch.LeaderVaultQueryEngine().Search(context.Background(), vaultQuery(v), nil)
	count := 0
	for _, err := range seq {
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// waitServedOnLeader waits until the node leading v serves exactly want of
// v's records. Serving other nodes is the query server's remote collection,
// covered at that layer. Progress metric: the served count.
func (h *orchRelHarness) waitServedOnLeader(v vaultSpec, want int) {
	h.t.Helper()
	leader := h.nodeIDs[v.nodeIdxs[0]]
	h.waitProgress(fmt.Sprintf("vault %s: %d records served by its leader %s", v.label, want, h.nodes[leader].label), 50*time.Millisecond,
		func() (string, bool) {
			got, err := h.servedCount(v, leader)
			if err != nil {
				return fmt.Sprintf("err(%v)", err), false
			}
			return fmt.Sprintf("served=%d", got), got == want
		}, func() { h.dumpPipelineState(v) })
}

// A memory vault is a pipeline vault: its leader serves its records, the
// sealed chunks and the open one alike, through the query path a file vault
// uses, and its sealed chunks are built on every home.
func TestOrchPipeline_MemoryVaultServesItsRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	t.Parallel()
	homeIdxs := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withMemoryVault(homeIdxs, "64MiB"),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	// The open chunk alone: a source-time search sorts it, since it has no
	// timestamp index to walk, rather than refusing it.
	const open = pipelineChunkMaxRecords / 2
	h.submitIngestRecords(h.nodeIDs[3], open, "memory-vault-open")
	h.waitOpenManifestRecords(v, h.nodeIDs[0], open)
	q := vaultQuery(v)
	q.OrderBy = query.OrderBySourceTS
	seq, _ := h.nodes[h.nodeIDs[0]].orch.LeaderVaultQueryEngine().Search(context.Background(), q, nil)
	bySource := 0
	for _, err := range seq {
		if err != nil {
			t.Fatalf("source-time search over the open chunk: %v", err)
		}
		bySource++
	}
	if bySource != open {
		t.Fatalf("source-time search over the open chunk served %d records, want %d", bySource, open)
	}

	// Two sealed chunks and an open one.
	total := 2*pipelineChunkMaxRecords + open
	h.submitIngestRecords(h.nodeIDs[3], 2*pipelineChunkMaxRecords, "memory-vault")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	h.waitOpenManifestRecords(v, h.nodeIDs[0], open)
	h.waitGLCBsOnHomes(v, homeIdxs, entries)
	h.waitServedOnLeader(v, total)

	// The anchor read behind the context view reads a sealed record on a home.
	rec, err := h.nodes[h.nodeIDs[0]].orch.LeaderVaultQueryEngine().ReadRecord(context.Background(), v.id, entries[0].ID, 0)
	if err != nil {
		t.Fatalf("ReadRecord of sealed chunk %s on a home: %v", entries[0].ID, err)
	}
	if len(rec.Raw) == 0 {
		t.Fatalf("ReadRecord of sealed chunk %s returned an empty record", entries[0].ID)
	}
}

// A memory vault's records survive a restart of its leader: its intake and
// sealed chunks are durable, and the restarted leader serves them again.
func TestOrchPipeline_MemoryVaultRecordsSurviveARestart(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node restart test")
	}
	t.Parallel()
	homeIdxs := []int{0, 1, 2, 3}
	h := newOrchRelHarness(t, 4,
		withMemoryVault(homeIdxs, "64MiB"),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	total := 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(h.nodeIDs[3], total, "memory-restart")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], int64(total))
	h.waitGLCBsOnHomes(v, homeIdxs, entries)
	h.waitServedOnLeader(v, total)

	// Restart the leader: its sealed chunks are read back into RAM from disk.
	restarted := h.nodeIDs[v.nodeIdxs[0]]
	h.stopNode(restarted)
	h.startNode(restarted)
	h.waitServedOnLeader(v, total)
}

// A memory vault's budget bounds its sealed chunks: over budget, retention
// retires the vault's real sealed chunks, oldest first, on the vault's leader
// and so on every home, while the open chunk's records stay served.
func TestOrchPipeline_MemoryBudgetRetiresTheVaultsSealedChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node retention test")
	}
	t.Parallel()
	homeIdxs := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		// One byte: every sealed chunk is over budget.
		withMemoryVault(homeIdxs, "1B"),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	const open = pipelineChunkMaxRecords / 2
	h.submitIngestRecords(h.nodeIDs[3], 2*pipelineChunkMaxRecords+open, "memory-budget")
	h.waitOpenManifestRecords(v, h.nodeIDs[0], open)

	// Every chunk that sealed is retired: gone from every node's manifest,
	// and its directory gone from every home's disk.
	h.waitProgress(fmt.Sprintf("vault %s: sealed chunks retired by the memory budget", v.label), 50*time.Millisecond,
		func() (string, bool) {
			openID, _ := h.openManifestRecords(v, h.nodeIDs[0])
			var views []string
			done := true
			for _, id := range h.nodeIDs {
				n := len(h.sealedPipelineChunks(v, id))
				views = append(views, fmt.Sprintf("%s sealed=%d", h.nodes[id].label, n))
				done = done && n == 0
			}
			for _, idx := range homeIdxs {
				nodeID := h.nodeIDs[idx]
				entries, err := os.ReadDir(filepath.Join(h.pipelineVaultRoot(nodeID, v), "chunks"))
				if err != nil && !os.IsNotExist(err) {
					return fmt.Sprintf("list chunk root on %s: %v", h.nodes[nodeID].label, err), false
				}
				var left []string
				for _, e := range entries {
					if e.Name() != openID.String() {
						left = append(left, e.Name())
					}
				}
				views = append(views, fmt.Sprintf("%s dirs=%v", h.nodes[nodeID].label, left))
				done = done && len(left) == 0
			}
			return fmt.Sprintf("%v", views), done
		}, func() { h.dumpPipelineState(v) })

	h.waitServedOnLeader(v, open)
}
