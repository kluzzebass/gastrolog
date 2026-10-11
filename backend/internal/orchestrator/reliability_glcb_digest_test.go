package orchestrator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/query"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

const buildDivergedAlarm = "chunking-build-diverged"

// plantedPayload is the payload of every record in a copy writeDivergentGLCB
// plants.
const plantedPayload = "divergent build"

// standingDivergenceAlarms lists the build-divergence alarms standing on a
// node.
func (h *orchRelHarness) standingDivergenceAlarms(nodeID string) []string {
	var out []string
	for _, a := range h.nodes[nodeID].alerts.Standing() {
		if strings.HasPrefix(a.ID, buildDivergedAlarm) {
			out = append(out, a.ID)
		}
	}
	return out
}

// waitHoldersOnEveryHome waits until a node's FSM records every home as a
// holder of every given chunk. A holder receipt is claimed only for a copy
// carrying the sealed digest. Progress metric: (chunk, home) receipts.
func (h *orchRelHarness) waitHoldersOnEveryHome(v vaultSpec, homeIdxs []int, ids []chunk.ChunkID) {
	h.t.Helper()
	total := len(ids) * len(homeIdxs)
	h.waitProgress(fmt.Sprintf("vault %s: holder receipts from %d homes for %d chunks", v.label, len(homeIdxs), len(ids)),
		50*time.Millisecond, func() (string, bool) {
			sub := h.vaultCtlSubFSM(v, h.nodeIDs[homeIdxs[0]])
			if sub == nil {
				return "fsm=<nil>", false
			}
			held := 0
			var missing []string
			for _, id := range ids {
				e := sub.Get(id)
				for _, idx := range homeIdxs {
					nodeID := h.nodeIDs[idx]
					if e != nil && slices.Contains(e.Holders, nodeID) {
						held++
					} else {
						missing = append(missing, fmt.Sprintf("%s@%s", id, h.nodes[nodeID].label))
					}
				}
			}
			return fmt.Sprintf("receipts=%d/%d missing=%v", held, total, missing), held == total
		}, func() { h.dumpPipelineState(v) })
}

func entryIDs(entries []vaultctlfsm.ManifestEntry) []chunk.ChunkID {
	out := make([]chunk.ChunkID, len(entries))
	for i, e := range entries {
		out[i] = e.ID
	}
	return out
}

// Every one of four homes builds every sealed chunk byte-identically, each
// copy's bytes hash to the digest the leader's seal recorded, every home
// claims a holder receipt, and no home raises the divergence alarm.
func TestOrchPipeline_EveryHomeCopyCarriesTheSealedDigest(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	homeIdxs := []int{0, 1, 2, 3}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homeIdxs),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	h.submitIngestRecords(h.nodeIDs[3], 2*pipelineChunkMaxRecords, "sealed-digest")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	h.waitGLCBsOnHomes(v, homeIdxs, entries)
	h.waitHoldersOnEveryHome(v, homeIdxs, entryIDs(entries))

	for _, idx := range homeIdxs {
		nodeID := h.nodeIDs[idx]
		for _, e := range h.sealedPipelineChunks(v, nodeID) {
			if !slices.ContainsFunc(entries, func(want vaultctlfsm.ManifestEntry) bool { return want.ID == e.ID && want.Hash == e.Hash }) {
				t.Errorf("home %s holds sealed digest %x for chunk %s, which differs from the other voters'", h.nodes[nodeID].label, e.Hash[:8], e.ID)
			}
		}
		if alarms := h.standingDivergenceAlarms(nodeID); len(alarms) != 0 {
			t.Errorf("home %s raised %v though every copy agrees", h.nodes[nodeID].label, alarms)
		}
	}
}

// writeDivergentGLCB writes a well-formed GLCB for chunkID holding records
// records that differ from anything ingested — a copy a home could build from
// segment bytes that differ from the leader's — and returns its digest.
func writeDivergentGLCB(t *testing.T, path string, vaultID glid.GLID, chunkID chunk.ChunkID, records uint32) [32]byte {
	t.Helper()
	at := time.Now().UTC()
	segID := glid.New()
	segPath := filepath.Join(t.TempDir(), segID.String())
	sf, err := segment.Create(segPath, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	for i := range records {
		rec := record.Record{
			EventID:  record.EventID{IngesterID: glid.New(), NodeID: glid.New(), IngestTS: at, IngestSeq: i},
			IngestTS: at,
			Raw:      []byte(plantedPayload),
		}
		if err := sf.Append(&rec, at); err != nil {
			t.Fatal(err)
		}
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
	// Built outside the home's chunk root: the home's recovery sweep deletes
	// build staging files it finds there.
	built := filepath.Join(t.TempDir(), "data.glcb")
	res, err := chunking.BuildGLCBFile(built, chunking.BuildGLCBInput{
		ChunkID: chunkID,
		VaultID: vaultID,
		Refs:    []chunking.SpanRef{{Path: segPath, Span: chunking.Span{SegmentID: segID, Start: 0, Count: records}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(built)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	staged := path + ".planted"
	if err := os.WriteFile(staged, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
	return res.BlobDigest
}

// A home whose copy of a chunk differs from the copy the seal agreed on —
// planted while the chunk is still open, so the home adopts it as its build —
// must not claim it, must raise the divergence alarm, and must end up holding
// the agreed copy, pulled from a peer and verified, and claimed.
//
// The planted home is chosen as a vault-ctl follower, but leadership can move
// before the seal. If it moved to the planted home, that home's copy is the
// one its seal recorded, and every other home is the divergent one; the
// mechanism under test is the same either way, so the test asserts whichever
// side the sealed digest puts each home on.
func TestOrchPipeline_DivergentHomeCopyIsReplacedByTheAgreedCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	homeIdxs := []int{0, 1, 2, 3}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homeIdxs),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	leader := h.waitForVaultCtlLeaderForVault(v)
	var planted string
	for _, idx := range homeIdxs {
		if id := h.nodeIDs[idx]; id != leader.id {
			planted = id
			break
		}
	}

	// Fill the open manifest one record short of sealing.
	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords-1, "divergent-before")
	var open chunk.ChunkID
	h.waitProgress("open manifest one record short of sealing", 50*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, planted)
		if sub == nil {
			return "fsm=<nil>", false
		}
		s, ok := sub.OpenChunkSummary()
		if !ok {
			return "open=<nil>", false
		}
		open = s.ChunkID
		return fmt.Sprintf("open=%s records=%d", s.ChunkID, s.TotalRecords), s.TotalRecords == pipelineChunkMaxRecords-1
	}, func() { h.dumpPipelineState(v) })

	plantedDigest := writeDivergentGLCB(t, h.pipelineGLCBPath(planted, v, open), v.id, open, pipelineChunkMaxRecords)

	h.submitIngestRecords(h.nodeIDs[3], 1, "divergent-after")
	entries := h.waitSealedRecords(v, h.nodeIDs[3], pipelineChunkMaxRecords)
	if len(entries) != 1 || entries[0].ID != open {
		t.Fatalf("sealed chunks = %v, want only the planted chunk %s", entryIDs(entries), open)
	}

	// Every home ends up holding byte-identical copies whose bytes hash to
	// the sealed digest, and every home claims one.
	h.waitHoldersOnEveryHome(v, homeIdxs, entryIDs(entries))
	h.waitGLCBsOnHomes(v, homeIdxs, entries)

	divergent := map[string]bool{planted: true}
	if entries[0].Hash == plantedDigest {
		t.Logf("vault-ctl leadership moved to %s before the seal: its planted copy is the agreed one", h.nodes[planted].label)
		divergent = map[string]bool{}
		for _, idx := range homeIdxs {
			if id := h.nodeIDs[idx]; id != planted {
				divergent[id] = true
			}
		}
	}
	want := []string{buildDivergedAlarm + ":" + v.id.String()}
	for _, idx := range homeIdxs {
		nodeID := h.nodeIDs[idx]
		alarms := h.standingDivergenceAlarms(nodeID)
		if !divergent[nodeID] {
			if len(alarms) != 0 {
				t.Errorf("home %s raised %v though its copy was the agreed one", h.nodes[nodeID].label, alarms)
			}
			continue
		}
		if !slices.Equal(alarms, want) {
			t.Errorf("divergent home %s alarms = %v, want %v", h.nodes[nodeID].label, alarms, want)
		}
		if _, err := os.Stat(h.pipelineGLCBPath(nodeID, v, open) + chunking.GLCBDivergentSuffix); !os.IsNotExist(err) {
			t.Errorf("home %s left its replaced divergent copy behind (stat=%v)", h.nodes[nodeID].label, err)
		}
	}

	// Every home serves the agreed copy's records, whichever copy it read
	// before its landing check.
	agreedPayloads := []string{"divergent-before", "divergent-after"}
	if !divergent[planted] {
		agreedPayloads = []string{plantedPayload}
	}
	for _, idx := range homeIdxs {
		nodeID := h.nodeIDs[idx]
		for _, raw := range servedPayloads(t, h, v, nodeID) {
			if !slices.ContainsFunc(agreedPayloads, func(p string) bool { return strings.HasPrefix(raw, p) }) {
				t.Fatalf("home %s served %q, a record of no agreed copy", h.nodes[nodeID].label, raw)
			}
		}
	}
}

// servedPayloads runs a match-all search on nodeID and returns every served
// record's payload.
func servedPayloads(t *testing.T, h *orchRelHarness, v vaultSpec, nodeID string) []string {
	t.Helper()
	seq, _, err := h.nodes[nodeID].orch.Search(context.Background(), v.id, query.Query{}, nil)
	if err != nil {
		t.Fatalf("search on %s: %v", h.nodes[nodeID].label, err)
	}
	var out []string
	for rec, err := range seq {
		if err != nil {
			t.Fatalf("search on %s: %v", h.nodes[nodeID].label, err)
		}
		out = append(out, string(rec.Raw))
	}
	return out
}
