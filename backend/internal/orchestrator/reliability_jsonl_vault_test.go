package orchestrator_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	chunkjsonl "gastrolog/internal/chunk/jsonl"
)

// sinkBodies counts each record payload in a JSONL sink file.
func sinkBodies(path string) (map[string]int, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]int{}, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	out := map[string]int{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var line struct {
			Raw string `json:"raw"`
		}
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			return nil, fmt.Errorf("sink line %q: %w", sc.Text(), err)
		}
		out[line.Raw]++
	}
	return out, sc.Err()
}

// waitSinkHolds waits until the sink holds exactly the bodies in want, once
// each. Progress metric: the sink's line count.
func (h *orchRelHarness) waitSinkHolds(path string, want map[string]bool) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("JSONL sink holding %d records once each", len(want)), 50*time.Millisecond,
		func() (string, bool) {
			got, err := sinkBodies(path)
			if err != nil {
				return fmt.Sprintf("err(%v)", err), false
			}
			lines := 0
			for body, n := range got {
				lines += n
				if n > 1 {
					h.t.Fatalf("JSONL sink holds %q %d times", body, n)
				}
				if !want[body] {
					h.t.Fatalf("JSONL sink holds %q, which is not a sealed record", body)
				}
			}
			return fmt.Sprintf("lines=%d", lines), lines == len(want)
		}, nil)
}

// A JSONL vault is a pipeline vault whose home writes each sealed chunk's
// records to its sink file once: records ingested on any node reach it, the
// open chunk's records wait for their seal, and a restart writes what the
// sink has not recorded without repeating what it has.
func TestOrchPipeline_JSONLVaultWritesEverySealedRecordOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	t.Parallel()
	path := filepath.Join(t.TempDir(), "sink.jsonl")
	h := newOrchRelHarness(t, 4,
		withJSONLVault(0, path),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	const open = pipelineChunkMaxRecords / 2
	bodies := h.submitIngestRecords(h.nodeIDs[3], 2*pipelineChunkMaxRecords, "jsonl-sealed")
	openBodies := h.submitIngestRecords(h.nodeIDs[3], open, "jsonl-open")
	h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	h.waitOpenManifestRecords(v, h.nodeIDs[0], open)
	h.waitSinkHolds(path, bodies)

	got, err := sinkBodies(path)
	if err != nil {
		t.Fatal(err)
	}
	for body := range openBodies {
		if got[body] != 0 {
			t.Fatalf("JSONL sink holds %q of the open chunk before it sealed", body)
		}
	}

	// Restart the sink's node with neither the sink nor its ledger: the home
	// holds landed chunks its sink never recorded, and with no new landing
	// its arrival writes every one of them.
	h.stopNode(h.nodeIDs[0])
	for _, p := range []string{path, path + chunkjsonl.LedgerSuffix} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	h.startNode(h.nodeIDs[0])
	h.waitSinkHolds(path, bodies)

	// The chunks sealed after it returns, the open one included, are written
	// once and nothing is written twice.
	more := h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords-open, "jsonl-after")
	for body := range openBodies {
		bodies[body] = true
	}
	for body := range more {
		bodies[body] = true
	}
	h.waitSealedRecords(v, h.nodeIDs[0], 3*pipelineChunkMaxRecords)
	h.waitSinkHolds(path, bodies)
}
