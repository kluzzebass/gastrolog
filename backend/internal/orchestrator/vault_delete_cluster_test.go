package orchestrator_test

// Deleting a vault removes, on every node, every byte the node keeps for it
// — staging segments, sealed chunks, chunk-manager files and its vault-ctl
// Raft group — or, without delete data, everything but the files holding its
// records. Other vaults keep every byte, including a vault whose ID differs
// from the deleted one's only in its last character.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
)

// withSiblingVault registers a vault placed on nodeIdxs whose ID is the ID of
// h.vaults[of] with its last byte flipped, so the two IDs' string forms share
// every character but the last.
func withSiblingVault(of int, nodeIdxs []int) orchRelOption {
	return func(h *orchRelHarness) {
		id := h.vaults[of].id
		id[len(id)-1] ^= 1
		h.vaults = append(h.vaults, vaultSpec{
			label:    string(rune('B' + len(h.vaults) - 1)),
			id:       id,
			nodeIdxs: nodeIdxs,
		})
	}
}

// deleteVaultEverywhere is the operator's delete as every live node applies
// it: the vault leaves every route (the delete RPC refuses a route
// destination), the config drops it, and each live node runs the dispatcher's
// vault-delete side effects.
func (h *orchRelHarness) deleteVaultEverywhere(v vaultSpec, deleteData bool) {
	h.t.Helper()
	h.dropVaultFromConfig(v, deleteData)
	for _, id := range h.nodeIDs {
		if n := h.nodes[id]; n.orch != nil {
			h.deleteVaultOn(id, v, deleteData)
		}
	}
}

// dropVaultFromConfig is the config half of the operator's delete: the vault
// leaves every route (the delete RPC refuses a route destination), every live
// node reloads its routing, and the config drops the vault.
func (h *orchRelHarness) dropVaultFromConfig(v vaultSpec, deleteData bool) {
	h.t.Helper()
	ctx := context.Background()
	routes, err := h.cfgStore.ListRoutes(ctx)
	if err != nil {
		h.t.Fatalf("ListRoutes: %v", err)
	}
	for _, rc := range routes {
		if !slices.Contains(rc.Destinations, v.id) {
			continue
		}
		rc.Destinations = slices.DeleteFunc(slices.Clone(rc.Destinations), func(id glid.GLID) bool { return id == v.id })
		if err := h.cfgStore.PutRoute(ctx, rc); err != nil {
			h.t.Fatalf("PutRoute %s: %v", rc.Name, err)
		}
	}
	h.reloadFiltersOnLiveNodes()
	if err := h.cfgStore.DeleteVault(ctx, v.id, deleteData); err != nil {
		h.t.Fatalf("cfgStore.DeleteVault %s: %v", v.label, err)
	}
}

// deleteVaultOn runs the dispatcher's vault-delete side effects on one node
// and waits for the vault's background teardown there.
func (h *orchRelHarness) deleteVaultOn(nodeID string, v vaultSpec, deleteData bool) {
	h.t.Helper()
	n := h.nodes[nodeID]
	if err := n.orch.DeleteVault(v.id, deleteData); err != nil {
		h.t.Fatalf("%s: DeleteVault %s: %v", n.label, v.label, err)
	}
	if err := n.orch.AwaitVaultTeardownForTest(context.Background(), v.id); err != nil {
		h.t.Fatalf("%s: vault %s teardown: %v", n.label, v.label, err)
	}
	if err := n.orch.ReloadFilters(context.Background()); err != nil {
		h.t.Fatalf("%s: ReloadFilters: %v", n.label, err)
	}
}

func (h *orchRelHarness) reloadFiltersOnLiveNodes() {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if n.orch == nil {
			continue
		}
		if err := n.orch.ReloadFilters(context.Background()); err != nil {
			h.t.Fatalf("%s: ReloadFilters: %v", n.label, err)
		}
	}
}

// restartAll stops every node, starts it again from its home, and waits for
// vault-ctl readiness.
func (h *orchRelHarness) restartAll() {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		h.stopNode(id)
	}
	for _, id := range h.nodeIDs {
		h.startNode(id)
	}
	h.waitForAllReady()
}

// vaultEntries lists, relative to the node's home, every file and directory
// whose path names the vault.
func vaultEntries(t *testing.T, n *orchRelNode, id glid.GLID) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(n.home, func(path string, _ fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			// The pipeline moved or released it mid-walk.
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(n.home, path)
		if err != nil {
			return err
		}
		if strings.Contains(rel, id.String()) {
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%s: walk home: %v", n.label, err)
	}
	return out
}

// vaultFileDigests maps every regular file whose path names the vault,
// relative to the node's home, to its content digest.
func vaultFileDigests(t *testing.T, n *orchRelNode, id glid.GLID) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	for _, rel := range vaultEntries(t, n, id) {
		path := filepath.Join(n.home, rel)
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("%s: stat %s: %v", n.label, rel, err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			t.Fatalf("%s: read %s: %v", n.label, rel, err)
		}
		out[rel] = sha256.Sum256(data)
	}
	return out
}

// assertNothingNamesVault fails for any file or directory on any node whose
// path names the vault — staging segments, chunk files, chunk-manager files,
// the vault-ctl group directory.
func (h *orchRelHarness) assertNothingNamesVault(what string, v vaultSpec) {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if left := vaultEntries(h.t, n, v.id); len(left) > 0 {
			h.t.Errorf("%s: %s still holds %d entries naming deleted vault %s, e.g. %v",
				what, n.label, len(left), v.label, left[:min(len(left), 8)])
		}
	}
}

// assertCtlGroupGoneFromWAL fails when a node's shared WAL still carries the
// deleted vault's vault-ctl group.
func (h *orchRelHarness) assertCtlGroupGoneFromWAL(v vaultSpec) {
	h.t.Helper()
	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if last, err := n.wal.GroupStore(gid).LastIndex(); err != nil || last != 0 {
			h.t.Errorf("%s: deleted vault %s's ctl group in the WAL: LastIndex=%d err=%v, want 0",
				n.label, v.label, last, err)
		}
	}
}

// snapshotVaultFiles captures every node's files naming the vault.
func (h *orchRelHarness) snapshotVaultFiles(v vaultSpec) map[string]map[string][32]byte {
	h.t.Helper()
	out := make(map[string]map[string][32]byte, len(h.nodeIDs))
	for _, id := range h.nodeIDs {
		out[id] = vaultFileDigests(h.t, h.nodes[id], v.id)
	}
	return out
}

// assertVaultFilesUnchanged fails for any file of the vault in before that is
// missing or changed, and for any file of the vault not in before.
func (h *orchRelHarness) assertVaultFilesUnchanged(what string, v vaultSpec, before map[string]map[string][32]byte) {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		now := vaultFileDigests(h.t, n, v.id)
		for rel, sum := range before[id] {
			got, ok := now[rel]
			switch {
			case !ok:
				h.t.Errorf("%s: %s lost vault %s file %s", what, n.label, v.label, rel)
			case got != sum:
				h.t.Errorf("%s: %s changed vault %s file %s", what, n.label, v.label, rel)
			}
		}
		for rel := range now {
			if _, ok := before[id][rel]; !ok {
				h.t.Errorf("%s: %s gained vault %s file %s", what, n.label, v.label, rel)
			}
		}
	}
}

// keptKind classifies a file naming a deleted vault by whether a delete
// without delete data keeps it.
type keptKind int

const (
	notKept       keptKind = iota
	keptSegment            // a staging segment
	keptChunkFile          // a file of a sealed chunk: pipeline chunk root or chunk-manager directory
)

// keptFileKind classifies a path relative to a node home.
func keptFileKind(rel string, id glid.GLID) keptKind {
	parts := strings.Split(rel, string(filepath.Separator))
	at := slices.Index(parts, id.String())
	if at < 0 {
		return notKept
	}
	tail := parts[at+1:]
	if len(tail) > 0 && tail[0] == id.String() {
		// <storage>/vaults/<id>/<id>/... is a file storage's chunk-manager dir.
		tail = tail[1:]
	}
	isChunkID := func(s string) bool {
		_, err := chunk.ParseChunkID(s)
		return err == nil
	}
	switch {
	case len(tail) == 2 && slices.Contains([]string{paths.Working, paths.Completed, paths.PreHead, paths.Head}, tail[0]):
		if _, err := glid.Parse(tail[1]); err == nil {
			return keptSegment
		}
	case len(tail) >= 3 && tail[0] == "chunks" && isChunkID(tail[1]):
		return keptChunkFile
	case len(tail) >= 2 && isChunkID(tail[0]):
		return keptChunkFile
	}
	return notKept
}

// assertOnlyRecordFilesKept fails for anything naming the vault that a
// delete without delete data must not keep: a file that holds no records
// (the chunk-manager lock, the vault-ctl group's files), or a directory that
// holds no file.
func (h *orchRelHarness) assertOnlyRecordFilesKept(what string, v vaultSpec) {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		for _, rel := range vaultEntries(h.t, n, v.id) {
			path := filepath.Join(n.home, rel)
			info, err := os.Lstat(path)
			if err != nil {
				h.t.Fatalf("%s: %s: stat %s: %v", what, n.label, rel, err)
			}
			if info.IsDir() {
				entries, err := os.ReadDir(path)
				if err != nil {
					h.t.Fatalf("%s: %s: read %s: %v", what, n.label, rel, err)
				}
				if len(entries) == 0 {
					h.t.Errorf("%s: %s keeps empty directory %s of deleted vault %s", what, n.label, rel, v.label)
				}
				continue
			}
			if keptFileKind(rel, v.id) == notKept {
				h.t.Errorf("%s: %s keeps %s of deleted vault %s, which holds no records", what, n.label, rel, v.label)
			}
		}
	}
}

// requireVaultOnDisk fails the test unless every node holds files of the
// vault, and every home also its vault-ctl group directory and a GLCB — the
// premise without which "nothing left" proves nothing.
func (h *orchRelHarness) requireVaultOnDisk(v vaultSpec, homeIdxs []int) {
	h.t.Helper()
	groupDir := filepath.Join("raft", "groups", raftgroup.VaultControlPlaneGroupID(v.id))
	for i, id := range h.nodeIDs {
		n := h.nodes[id]
		entries := vaultEntries(h.t, n, v.id)
		if len(entries) == 0 {
			h.t.Fatalf("premise: %s holds no files of vault %s before the delete", n.label, v.label)
		}
		if !slices.Contains(entries, groupDir) {
			h.t.Fatalf("premise: %s has no vault-ctl group directory %s for vault %s", n.label, groupDir, v.label)
		}
		if !slices.Contains(homeIdxs, i) {
			continue
		}
		hasGLCB := slices.ContainsFunc(entries, func(rel string) bool { return strings.HasSuffix(rel, "data.glcb") })
		if !hasGLCB {
			h.t.Fatalf("premise: home %s holds no GLCB of vault %s before the delete", n.label, v.label)
		}
	}
}

// ingestStream submits records until stopped and returns the bodies whose
// durability ack succeeded.
type ingestStream struct {
	stop   chan struct{}
	done   chan struct{}
	mu     sync.Mutex
	bodies map[string]bool
}

func (s *ingestStream) finish() map[string]bool {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bodies
}

// streamIngest submits records through the routing path on nodeID until the
// stream is finished.
func (h *orchRelHarness) streamIngest(nodeID, prefix string) *ingestStream {
	s := &ingestStream{stop: make(chan struct{}), done: make(chan struct{}), bodies: map[string]bool{}}
	n := h.nodes[nodeID]
	go func() {
		defer close(s.done)
		for i := 0; ; i++ {
			select {
			case <-s.stop:
				return
			default:
			}
			body := fmt.Sprintf("%s-%05d", prefix, i)
			ack := make(chan error, 1)
			rec := chunk.Record{IngestTS: time.Now().UTC(), Raw: []byte(body)}
			if err := n.orch.SubmitIngest(context.Background(), rec, ack); err != nil {
				select {
				case <-s.stop:
					return
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}
			select {
			case err := <-ack:
				if err == nil {
					s.mu.Lock()
					s.bodies[body] = true
					s.mu.Unlock()
				}
			case <-time.After(orchHarnessEventTimeout):
				return
			}
		}
	}()
	return s
}

// streamToVault writes records straight into the vault's segmentation queue
// on nodeID, as ImportRecords does, until the stream is finished or the vault
// stops accepting them.
func (h *orchRelHarness) streamToVault(nodeID string, v vaultSpec, prefix string) *ingestStream {
	s := &ingestStream{stop: make(chan struct{}), done: make(chan struct{}), bodies: map[string]bool{}}
	n := h.nodes[nodeID]
	go func() {
		defer close(s.done)
		for i := 0; ; i++ {
			select {
			case <-s.stop:
				return
			default:
			}
			ack := make(chan error, 1)
			rec := chunk.Record{IngestTS: time.Now().UTC(), Raw: fmt.Appendf(nil, "%s-%05d", prefix, i)}
			if err := n.orch.SubmitToVault(context.Background(), v.id, rec, ack); err != nil {
				return
			}
			select {
			case <-ack:
			case <-time.After(orchHarnessEventTimeout):
				return
			}
		}
	}()
	return s
}

// sealAllIngested ingests through nodeID until the number of records the
// routes have taken is a whole number of chunks, so every record ends sealed
// and searchable. bodies gains the new records.
func (h *orchRelHarness) sealAllIngested(nodeID string, bodies map[string]bool, prefix string) {
	h.t.Helper()
	if short := len(bodies) % pipelineChunkMaxRecords; short != 0 {
		for body := range h.submitIngestRecords(nodeID, pipelineChunkMaxRecords-short, prefix) {
			bodies[body] = true
		}
	}
}

// glcbDigests captures the GLCB of every sealed entry on every home.
func (h *orchRelHarness) glcbDigests(v vaultSpec, homeIdxs []int) map[string][32]byte {
	h.t.Helper()
	out := map[string][32]byte{}
	for _, idx := range homeIdxs {
		id := h.nodeIDs[idx]
		for _, e := range h.sealedPipelineChunks(v, id) {
			path := h.pipelineGLCBPath(id, v, e.ID)
			data, err := os.ReadFile(filepath.Clean(path))
			if err != nil {
				h.t.Fatalf("%s: read GLCB %s of vault %s: %v", h.nodes[id].label, e.ID, v.label, err)
			}
			out[path] = sha256.Sum256(data)
		}
	}
	return out
}

func assertGLCBDigests(t *testing.T, what string, want map[string][32]byte) {
	t.Helper()
	for path, sum := range want {
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			t.Errorf("%s: GLCB %s gone: %v", what, path, err)
			continue
		}
		if got := sha256.Sum256(data); !bytes.Equal(got[:], sum[:]) {
			t.Errorf("%s: GLCB %s changed", what, path)
		}
	}
}

// A vault deleted with its data while records are still being written to it
// leaves nothing on any node — not on its homes, not on the node that only
// originated segments for it — and nothing comes back on restart. The vault
// whose ID differs only in its last character, homed on the same nodes and
// still being ingested, keeps every GLCB byte-identical and serves every
// record it acked.
func TestOrchVaultDeleteWithDataLeavesNothingOnAnyNode(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault-delete acceptance test with node restarts")
	}
	t.Parallel()

	homeIdxs := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homeIdxs),
		withSiblingVault(1, homeIdxs),
		withMatchAllRoute(1, 2),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	doomed, sibling := h.vaults[1], h.vaults[2]
	originOnly := h.nodeIDs[3]

	const sealed = 2 * pipelineChunkMaxRecords
	siblingBodies := h.submitIngestRecords(originOnly, sealed, "before-delete")
	for _, v := range []vaultSpec{doomed, sibling} {
		h.waitGLCBsOnHomes(v, homeIdxs, h.waitSealedRecords(v, h.nodeIDs[0], sealed))
	}
	h.requireVaultOnDisk(doomed, homeIdxs)
	siblingGLCBs := h.glcbDigests(sibling, homeIdxs)

	viaRoute := h.streamIngest(originOnly, "during-delete")
	direct := h.streamToVault(h.nodeIDs[0], doomed, "direct")
	h.deleteVaultEverywhere(doomed, true)
	direct.finish()
	for body := range viaRoute.finish() {
		siblingBodies[body] = true
	}
	h.sealAllIngested(originOnly, siblingBodies, "after-delete")

	h.assertNothingNamesVault("after the delete", doomed)
	assertGLCBDigests(t, "sibling after the delete", siblingGLCBs)
	h.waitSearchable(sibling, h.nodeIDs[1], len(siblingBodies))
	h.assertSearchBodiesExactly(sibling, h.nodeIDs[1], siblingBodies)

	h.restartAll()
	h.assertNothingNamesVault("after restart", doomed)
	h.assertCtlGroupGoneFromWAL(doomed)
	assertGLCBDigests(t, "sibling after restart", siblingGLCBs)
	h.waitSearchable(sibling, h.nodeIDs[1], len(siblingBodies))
	h.assertSearchBodiesExactly(sibling, h.nodeIDs[1], siblingBodies)
}

// A vault deleted without its data keeps exactly the files holding its
// records — staging segments and sealed chunk files, byte-identical — on every
// node, and nothing else: no lock file, no vault-ctl group directory, no
// empty directory. A restart neither removes nor recreates any of it.
func TestOrchVaultDeleteWithoutDataKeepsOnlyRecordFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault-delete acceptance test with node restarts")
	}
	t.Parallel()

	homeIdxs := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homeIdxs),
		withSiblingVault(1, homeIdxs),
		withMatchAllRoute(1, 2),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	kept, sibling := h.vaults[1], h.vaults[2]
	originOnly := h.nodeIDs[3]

	// Sealed chunks, then an open one: GLCBs on the homes and segments not
	// yet chunked in their staging areas.
	const sealed, open = 2 * pipelineChunkMaxRecords, pipelineChunkMaxRecords / 2
	bodies := h.submitIngestRecords(originOnly, sealed, "sealed")
	for _, v := range []vaultSpec{kept, sibling} {
		h.waitGLCBsOnHomes(v, homeIdxs, h.waitSealedRecords(v, h.nodeIDs[0], sealed))
	}
	for body := range h.submitIngestRecords(originOnly, open, "open") {
		bodies[body] = true
	}
	for _, v := range []vaultSpec{kept, sibling} {
		h.waitOpenManifestRecords(v, h.nodeIDs[0], open)
	}
	h.requireVaultOnDisk(kept, homeIdxs)
	keptGLCBs := h.glcbDigests(kept, homeIdxs)
	siblingGLCBs := h.glcbDigests(sibling, homeIdxs)

	h.deleteVaultEverywhere(kept, false)

	h.assertOnlyRecordFilesKept("after the delete", kept)
	assertGLCBDigests(t, "kept vault after the delete", keptGLCBs)
	afterDelete := h.snapshotVaultFiles(kept)
	segments := 0
	for _, byPath := range afterDelete {
		for rel := range byPath {
			if keptFileKind(rel, kept.id) == keptSegment {
				segments++
			}
		}
	}
	if segments == 0 {
		t.Errorf("no staging segment of the deleted vault was kept; the open chunk's records were in them")
	}
	assertGLCBDigests(t, "sibling after the delete", siblingGLCBs)
	h.sealAllIngested(originOnly, bodies, "after-delete")
	h.waitSearchable(sibling, h.nodeIDs[1], len(bodies))
	h.assertSearchBodiesExactly(sibling, h.nodeIDs[1], bodies)

	h.restartAll()
	h.assertVaultFilesUnchanged("after restart", kept, afterDelete)
	h.assertOnlyRecordFilesKept("after restart", kept)
	h.assertCtlGroupGoneFromWAL(kept)
	h.waitSearchable(sibling, h.nodeIDs[1], len(bodies))
	h.assertSearchBodiesExactly(sibling, h.nodeIDs[1], bodies)
}

// A home that is down when the vault is deleted keeps the vault's files
// until the deletion reaches it; when it comes back and applies the delete
// — the config log replay the dispatcher sees — they go, and a restart
// afterwards finds nothing of the vault, WAL included.
func TestOrchVaultDeleteReachesNodeDownAtDeleteTime(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault-delete acceptance test with node restarts")
	}
	t.Parallel()

	homeIdxs := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homeIdxs),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	const sealed = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(h.nodeIDs[3], sealed, "down-at-delete")
	h.waitGLCBsOnHomes(v, homeIdxs, h.waitSealedRecords(v, h.nodeIDs[0], sealed))
	h.requireVaultOnDisk(v, homeIdxs)

	down := h.nodes[h.nodeIDs[2]]
	h.stopNode(down.id)
	h.deleteVaultEverywhere(v, true)
	h.startNode(down.id)
	h.waitForAllReady()

	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		left := vaultEntries(t, n, v.id)
		switch {
		case n == down && len(left) == 0:
			t.Fatalf("%s removed vault %s's files before the deletion reached it", n.label, v.label)
		case n != down && len(left) > 0:
			t.Errorf("%s still holds vault %s files: %v", n.label, v.label, left[:min(len(left), 8)])
		}
	}

	h.deleteVaultOn(down.id, v, true)
	h.assertNothingNamesVault("after the returning node applied the delete", v)

	h.stopNode(down.id)
	h.startNode(down.id)
	h.waitForAllReady()
	h.assertNothingNamesVault("after the returning node restarted", v)
	h.assertCtlGroupGoneFromWAL(v)
}

// On a single node, a file vault and a memory vault deleted with their data
// leave nothing; deleted without it they keep exactly their record files.
func TestOrchVaultDeleteSingleNodeFileAndMemoryVaults(t *testing.T) {
	if testing.Short() {
		t.Skip("pipeline vault-delete acceptance test")
	}
	t.Parallel()

	for _, deleteData := range []bool{true, false} {
		t.Run(fmt.Sprintf("delete_data_%t", deleteData), func(t *testing.T) {
			t.Parallel()
			h := newOrchRelHarness(t, 1,
				withMemoryVault([]int{0}, "64MiB"),
				withMatchAllRoute(0, 1),
				withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
			)
			fileVault, memoryVault := h.vaults[0], h.vaults[1]
			node := h.nodeIDs[0]

			const sealed, open = 2 * pipelineChunkMaxRecords, pipelineChunkMaxRecords / 2
			h.submitIngestRecords(node, sealed+open, "single-node")
			glcbs := map[string]map[string][32]byte{}
			for _, v := range []vaultSpec{fileVault, memoryVault} {
				h.waitGLCBsOnHomes(v, []int{0}, h.waitSealedRecords(v, node, sealed))
				h.waitOpenManifestRecords(v, node, open)
				h.requireVaultOnDisk(v, []int{0})
				glcbs[v.label] = h.glcbDigests(v, []int{0})
			}

			afterDelete := map[string]map[string]map[string][32]byte{}
			for _, v := range []vaultSpec{fileVault, memoryVault} {
				h.deleteVaultEverywhere(v, deleteData)
				if deleteData {
					h.assertNothingNamesVault(v.label+" after the delete", v)
					continue
				}
				h.assertOnlyRecordFilesKept(v.label+" after the delete", v)
				assertGLCBDigests(t, v.label+" after the delete", glcbs[v.label])
				afterDelete[v.label] = h.snapshotVaultFiles(v)
			}

			h.restartAll()
			for _, v := range []vaultSpec{fileVault, memoryVault} {
				if deleteData {
					h.assertNothingNamesVault(v.label+" after restart", v)
				} else {
					h.assertVaultFilesUnchanged(v.label+" after restart", v, afterDelete[v.label])
					assertGLCBDigests(t, v.label+" after restart", glcbs[v.label])
				}
				h.assertCtlGroupGoneFromWAL(v)
			}
		})
	}
}

// addCloudVaultsWithOneRoute adds cloud-backed vaults on one shared cloud
// service, placed on nodeIdxs, every one a destination of a single match-all
// route. The IDs come from ids.
func (h *orchRelHarness) addCloudVaultsWithOneRoute(labels []string, ids []glid.GLID, nodeIdxs []int) []vaultSpec {
	h.t.Helper()
	ctx := context.Background()
	csID := glid.New()
	if err := h.cfgStore.PutCloudService(ctx, system.CloudService{ID: csID, Name: "orch-rel-delete-cloud", Provider: "memory"}); err != nil {
		h.t.Fatalf("PutCloudService: %v", err)
	}
	var out []vaultSpec
	for i, label := range labels {
		v := vaultSpec{label: label, id: ids[i], nodeIdxs: nodeIdxs}
		if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
			Enabled:          true,
			ID:               v.id,
			Name:             "orch-rel-vault-" + label,
			Type:             system.VaultTypeFile,
			StorageClass:     harnessStorageClass,
			RotationPolicyID: h.rotationPolicyID,
			CloudServiceID:   &csID,
			CacheEviction:    "lru",
			CacheBudget:      "1GiB",
		}); err != nil {
			h.t.Fatalf("PutVault %s: %v", label, err)
		}
		h.setVaultPlacements(v, nodeIdxs)
		h.vaults = append(h.vaults, v)
		out = append(out, v)
	}
	dests := make([]glid.GLID, 0, len(out))
	for _, v := range out {
		dests = append(dests, v.id)
	}
	if err := h.cfgStore.PutRoute(ctx, system.RouteConfig{
		ID:           glid.New(),
		Name:         "orch-rel-route-cloud",
		Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
		Destinations: dests,
		Enabled:      true,
	}); err != nil {
		h.t.Fatalf("PutRoute: %v", err)
	}
	sys, err := h.cfgStore.Load(ctx)
	if err != nil {
		h.t.Fatalf("Load: %v", err)
	}
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		for _, v := range out {
			for i := range sys.Config.Vaults {
				if sys.Config.Vaults[i].ID != v.id {
					continue
				}
				if err := n.orch.AddVault(ctx, sys.Config.Vaults[i], n.factories); err != nil {
					h.t.Fatalf("AddVault %s on %s: %v", v.label, n.label, err)
				}
			}
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			h.t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
	}
	return out
}

// cloudKeys lists the bucket's object keys that name the vault.
func cloudKeys(t *testing.T, store blobstore.Store, v vaultSpec) []string {
	t.Helper()
	var keys []string
	if err := store.List(context.Background(), "", func(b blobstore.BlobInfo) error {
		if strings.Contains(b.Key, v.id.String()) {
			keys = append(keys, b.Key)
		}
		return nil
	}); err != nil {
		t.Fatalf("list bucket: %v", err)
	}
	slices.Sort(keys)
	return keys
}

// A cloud-backed vault deleted with its data loses its cloud objects too;
// deleted without it, its objects stay. The cloud vault whose ID differs from
// the first only in its last character keeps every object.
func TestOrchVaultDeleteCloudObjectsFollowDeleteData(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault-delete acceptance test")
	}
	t.Parallel()

	bucket := blobstore.NewMemory()
	h := newOrchRelHarness(t, 4,
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
		withSharedCloudStore(bucket),
	)
	homeIdxs := []int{0, 1, 2}
	erasedID, keptID := glid.New(), glid.New()
	siblingID := erasedID
	siblingID[len(siblingID)-1] ^= 1
	vs := h.addCloudVaultsWithOneRoute([]string{"E", "K", "S"}, []glid.GLID{erasedID, keptID, siblingID}, homeIdxs)
	erased, kept, sibling := vs[0], vs[1], vs[2]

	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(h.nodeIDs[3], total, "cloud-delete")
	for _, v := range vs {
		entries := h.waitSealedRecords(v, h.nodeIDs[0], total)
		h.waitProgress(fmt.Sprintf("vault %s: every sealed chunk uploaded and cloud-backed on every home", v.label), 100*time.Millisecond,
			func() (string, bool) {
				backed := 0
				for _, idx := range homeIdxs {
					sub := h.vaultCtlSubFSM(v, h.nodeIDs[idx])
					if sub == nil {
						continue
					}
					for _, want := range entries {
						if e := sub.Get(want.ID); e != nil && e.CloudBacked {
							backed++
						}
					}
				}
				n := len(cloudKeys(t, bucket, v))
				return fmt.Sprintf("objects=%d/%d cloud_backed=%d/%d", n, len(entries), backed, len(entries)*len(homeIdxs)),
					n == len(entries) && backed == len(entries)*len(homeIdxs)
			}, func() { h.dumpPipelineState(v) })
	}
	keptKeys, siblingKeys := cloudKeys(t, bucket, kept), cloudKeys(t, bucket, sibling)

	h.deleteVaultEverywhere(erased, true)
	h.deleteVaultEverywhere(kept, false)

	if left := cloudKeys(t, bucket, erased); len(left) > 0 {
		t.Errorf("vault deleted with its data keeps cloud objects: %v", left)
	}
	if got := cloudKeys(t, bucket, kept); !slices.Equal(got, keptKeys) {
		t.Errorf("vault deleted without its data: cloud objects %v, want %v", got, keptKeys)
	}
	if got := cloudKeys(t, bucket, sibling); !slices.Equal(got, siblingKeys) {
		t.Errorf("sibling vault's cloud objects %v, want %v", got, siblingKeys)
	}
	h.assertNothingNamesVault("after the delete", erased)
	h.assertOnlyRecordFilesKept("after the delete", kept)
}

// gatedUploadStore holds every Upload until released or until the upload's
// context ends, reporting when an upload is in flight and when it returned.
type gatedUploadStore struct {
	blobstore.Store
	gate     chan struct{}
	started  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func newGatedUploadStore(inner blobstore.Store) *gatedUploadStore {
	return &gatedUploadStore{
		Store:    inner,
		gate:     make(chan struct{}),
		started:  make(chan struct{}, 1),
		returned: make(chan struct{}, 1),
	}
}

func (s *gatedUploadStore) Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	defer func() {
		select {
		case s.returned <- struct{}{}:
		default:
		}
	}()
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Store.Upload(ctx, key, data, metadata)
}

func (s *gatedUploadStore) release() { s.once.Do(func() { close(s.gate) }) }

func awaitSignal(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(orchHarnessEventTimeout):
		// One event that either happens or does not: no progress to watch.
		t.Fatalf("%s: no signal within %s", what, orchHarnessEventTimeout)
	}
}

// A cloud upload in flight when its vault is deleted with its data does not
// land afterwards: the delete stops the vault's uploads and waits for the
// in-flight one to return before it deletes the vault's objects, so the
// object the upload was writing never appears.
func TestOrchVaultDeleteStopsInFlightCloudUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("pipeline vault-delete acceptance test")
	}
	t.Parallel()

	bucket := blobstore.NewMemory()
	gated := newGatedUploadStore(bucket)
	h := newOrchRelHarness(t, 1,
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
		withSharedCloudStore(gated),
	)
	v := h.addCloudVaultsWithOneRoute([]string{"U"}, []glid.GLID{glid.New()}, []int{0})[0]

	h.submitIngestRecords(h.nodeIDs[0], pipelineChunkMaxRecords, "upload-in-flight")
	h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	awaitSignal(t, "cloud upload in flight", gated.started)

	h.deleteVaultEverywhere(v, true)
	gated.release()
	awaitSignal(t, "in-flight cloud upload returned", gated.returned)

	if left := cloudKeys(t, bucket, v); len(left) > 0 {
		t.Fatalf("an upload in flight at the delete landed afterwards: %v", left)
	}
	h.assertNothingNamesVault("after the delete", v)
}

// holdingLoader is a config loader that a test can pause right after one read,
// so the read's caller applies config read before whatever the test does in
// the pause.
type holdingLoader struct {
	inner orchestrator.SystemLoader
	mu    sync.Mutex
	held  *loadHold
}

// loadHold pauses one config read: loaded is closed once the read is done,
// and the reader waits for proceed to be closed.
type loadHold struct {
	loaded  chan struct{}
	proceed chan struct{}
}

func (l *holdingLoader) Load(ctx context.Context) (*system.System, error) {
	sys, err := l.inner.Load(ctx)
	l.mu.Lock()
	hold := l.held
	l.held = nil
	l.mu.Unlock()
	if hold != nil {
		close(hold.loaded)
		<-hold.proceed
	}
	return sys, err
}

// holdNextLoad pauses the next config read on the node after it has read.
func (l *holdingLoader) holdNextLoad() *loadHold {
	hold := &loadHold{loaded: make(chan struct{}), proceed: make(chan struct{})}
	l.mu.Lock()
	l.held = hold
	l.mu.Unlock()
	return hold
}

// Config read before a vault's deletion still names the vault. A routing
// reload and a vault add that read config, then apply it after the deletion,
// must not bring the vault back: no pipeline registration, no vault, no
// files, on the home that applied them.
func TestOrchVaultDeleteFencesConfigReadBeforeIt(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault-delete acceptance test")
	}
	t.Parallel()

	homeIdxs := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homeIdxs),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	const sealed = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(h.nodeIDs[3], sealed, "stale-config")
	h.waitGLCBsOnHomes(v, homeIdxs, h.waitSealedRecords(v, h.nodeIDs[0], sealed))
	h.requireVaultOnDisk(v, homeIdxs)
	vaultCfg, err := h.cfgStore.GetVault(context.Background(), v.id)
	if err != nil || vaultCfg == nil {
		t.Fatalf("GetVault: %v", err)
	}

	reloader, adder := h.nodes[h.nodeIDs[0]], h.nodes[h.nodeIDs[1]]
	reloadHold := reloader.loader.holdNextLoad()
	reloadErr := make(chan error, 1)
	go func() { reloadErr <- reloader.orch.ReloadFilters(context.Background()) }()
	awaitSignal(t, "routing reload read config", reloadHold.loaded)

	addHold := adder.loader.holdNextLoad()
	addErr := make(chan error, 1)
	go func() { addErr <- adder.orch.AddVault(context.Background(), *vaultCfg, adder.factories) }()
	awaitSignal(t, "vault add read config", addHold.loaded)

	// The route still names the vault in the held reads; the delete drops it
	// from the route first, as the delete RPC requires.
	h.deleteVaultEverywhere(v, true)
	close(reloadHold.proceed)
	close(addHold.proceed)
	if err := <-reloadErr; err != nil {
		t.Fatalf("routing reload: %v", err)
	}
	if err := <-addErr; !errors.Is(err, orchestrator.ErrVaultNotFound) {
		t.Errorf("vault add from config read before the delete: err=%v, want ErrVaultNotFound", err)
	}

	for _, n := range []*orchRelNode{reloader, adder} {
		if slices.Contains(n.orch.ListVaults(), v.id) {
			t.Errorf("%s: deleted vault %s registered again", n.label, v.label)
		}
	}
	h.assertNothingNamesVault("after config read before the delete was applied", v)
}

// listGatedStore holds the next List under one key prefix until released,
// reporting when one is held and how the hold ended: a cloud store that stops
// answering mid-delete.
type listGatedStore struct {
	blobstore.Store
	mu      sync.Mutex
	prefix  string
	gate    chan struct{}
	holding chan struct{}
	ended   chan error
}

// holdNextList makes the next List under prefix wait for release; holding is
// closed when it starts waiting, and ended receives nil when it was released
// or its context's error when that ended it first.
func (s *listGatedStore) holdNextList(prefix string) (holding <-chan struct{}, ended <-chan error, release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prefix = prefix
	s.gate = make(chan struct{})
	s.holding = make(chan struct{})
	s.ended = make(chan error, 1)
	gate := s.gate
	return s.holding, s.ended, sync.OnceFunc(func() { close(gate) })
}

func (s *listGatedStore) List(ctx context.Context, prefix string, fn func(blobstore.BlobInfo) error) error {
	s.mu.Lock()
	var gate, holding chan struct{}
	var ended chan error
	if s.gate != nil && strings.HasPrefix(prefix, s.prefix) {
		gate, holding, ended = s.gate, s.holding, s.ended
		s.gate, s.holding, s.ended = nil, nil, nil
	}
	s.mu.Unlock()
	if gate != nil {
		close(holding)
		select {
		case <-gate:
			ended <- nil
		case <-ctx.Done():
			ended <- ctx.Err()
			return ctx.Err()
		}
	}
	return s.Store.List(ctx, prefix, fn)
}

// blockedStoreHarness is a one-node cluster whose cloud vaults, one per
// label, share a bucket that can stop answering, each holding uploaded
// sealed chunks.
func blockedStoreHarness(t *testing.T, labels ...string) (*orchRelHarness, *listGatedStore, []vaultSpec) {
	t.Helper()
	bucket := &listGatedStore{Store: blobstore.NewMemory()}
	h := newOrchRelHarness(t, 1,
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
		withSharedCloudStore(bucket),
	)
	ids := make([]glid.GLID, len(labels))
	for i := range ids {
		ids[i] = glid.New()
	}
	vs := h.addCloudVaultsWithOneRoute(labels, ids, []int{0})
	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(h.nodeIDs[0], total, "blocked-store")
	for _, v := range vs {
		entries := h.waitSealedRecords(v, h.nodeIDs[0], total)
		h.waitProgress(fmt.Sprintf("vault %s: every sealed chunk uploaded", v.label), 100*time.Millisecond,
			func() (string, bool) {
				n := len(cloudKeys(t, bucket, v))
				return fmt.Sprintf("objects=%d/%d", n, len(entries)), n == len(entries)
			}, func() { h.dumpPipelineState(v) })
	}
	return h, bucket, vs
}

// holdPurge makes the vault's next cloud purge wait on the store.
func holdPurge(bucket *listGatedStore, v vaultSpec) (holding <-chan struct{}, ended <-chan error, release func()) {
	return bucket.holdNextList("vault-" + v.id.String() + "/")
}

// A vault delete whose teardown waits on an unresponsive cloud store returns
// at once — it runs on the config apply goroutine — and neither it nor the
// waiting teardown holds the orchestrator lock: the registry is readable, the
// teardown is a visible scheduler job, and a config change that follows
// applies while it waits. Once the store answers the teardown removes the
// vault's files and objects; deletes back to back complete the same way.
func TestOrchVaultDeleteReturnsWhileTeardownWaitsOnCloudStore(t *testing.T) {
	if testing.Short() {
		t.Skip("pipeline vault-delete acceptance test")
	}
	t.Parallel()

	h, bucket, vs := blockedStoreHarness(t, "P", "Q", "R")
	node := h.nodes[h.nodeIDs[0]]
	blocked, kept := vs[0], vs[2]
	keptKeys := cloudKeys(t, bucket, kept)

	h.dropVaultFromConfig(blocked, true)
	holding, _, release := holdPurge(bucket, blocked)
	defer release()
	if err := node.orch.DeleteVault(blocked.id, true); err != nil {
		t.Fatalf("DeleteVault %s: %v", blocked.label, err)
	}
	awaitSignal(t, "teardown waiting on the cloud store", holding)

	if !node.orch.TryLockForTest() {
		t.Errorf("the teardown holds the orchestrator lock while it waits on the cloud store")
	}
	if slices.Contains(node.orch.ListVaults(), blocked.id) {
		t.Errorf("deleted vault %s still registered", blocked.label)
	}
	if !node.orch.Scheduler().HasPendingPrefix("vault-teardown:" + blocked.id.String()) {
		t.Errorf("the pending teardown of vault %s is not a scheduler job", blocked.label)
	}
	added := h.addCloudVaultsWithOneRoute([]string{"N"}, []glid.GLID{glid.New()}, []int{0})[0]
	if !slices.Contains(node.orch.ListVaults(), added.id) {
		t.Errorf("vault created while a teardown waits is not registered")
	}

	release()
	if err := node.orch.AwaitVaultTeardownForTest(context.Background(), blocked.id); err != nil {
		t.Fatalf("teardown of %s: %v", blocked.label, err)
	}
	h.deleteVaultEverywhere(vs[1], true)
	h.deleteVaultEverywhere(kept, false)

	for _, v := range vs[:2] {
		if left := cloudKeys(t, bucket, v); len(left) > 0 {
			t.Errorf("vault %s deleted with its data keeps cloud objects: %v", v.label, left)
		}
		h.assertNothingNamesVault("vault "+v.label+" after the delete", v)
	}
	if got := cloudKeys(t, bucket, kept); !slices.Equal(got, keptKeys) {
		t.Errorf("vault deleted without its data: cloud objects %v, want %v", got, keptKeys)
	}
	h.assertOnlyRecordFilesKept("after the delete", kept)
}

// A vault re-created under the ID of a deleted vault whose teardown is still
// pending shares that vault's directories and vault-ctl group ID. The
// re-create waits for the teardown, so the teardown never removes the new
// vault's files: the new vault ingests, seals and keeps its GLCBs.
func TestOrchVaultRecreateWaitsForPendingTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("pipeline vault-delete acceptance test")
	}
	t.Parallel()

	h, bucket, vs := blockedStoreHarness(t, "P")
	node := h.nodes[h.nodeIDs[0]]
	v := vs[0]
	ctx := context.Background()
	vaultCfg, err := h.cfgStore.GetVault(ctx, v.id)
	if err != nil || vaultCfg == nil {
		t.Fatalf("GetVault: %v", err)
	}
	routes, err := h.cfgStore.ListRoutes(ctx)
	if err != nil {
		t.Fatal(err)
	}

	h.dropVaultFromConfig(v, true)
	holding, _, release := holdPurge(bucket, v)
	defer release()
	if err := node.orch.DeleteVault(v.id, true); err != nil {
		t.Fatalf("DeleteVault: %v", err)
	}
	awaitSignal(t, "teardown waiting on the cloud store", holding)

	// Re-create the vault, same ID, same route, while its teardown waits.
	if err := h.cfgStore.PutVault(ctx, *vaultCfg); err != nil {
		t.Fatal(err)
	}
	h.setVaultPlacements(v, v.nodeIdxs)
	for _, rc := range routes {
		if err := h.cfgStore.PutRoute(ctx, rc); err != nil {
			t.Fatal(err)
		}
	}
	added := make(chan error, 1)
	go func() { added <- node.orch.AddVault(ctx, *vaultCfg, node.factories) }()
	release()
	if err := <-added; err != nil {
		t.Fatalf("re-create AddVault: %v", err)
	}
	if err := node.orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}

	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(node.id, total, "recreated")
	entries := h.waitSealedRecords(v, node.id, total)
	h.waitGLCBsOnHomes(v, v.nodeIdxs, entries)
	if err := node.orch.AwaitVaultTeardownForTest(ctx, v.id); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, err := os.Stat(h.pipelineGLCBPath(node.id, v, e.ID)); err != nil {
			t.Errorf("re-created vault lost GLCB %s: %v", e.ID, err)
		}
	}
	if !slices.Contains(node.orch.ListVaults(), v.id) {
		t.Errorf("re-created vault not registered")
	}
}

// Shutdown with a vault teardown waiting on an unresponsive cloud store
// abandons the teardown: the wait ends by cancellation, not by the store's
// own timeout, and Stop returns.
func TestOrchStopAbandonsPendingVaultTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("pipeline vault-delete acceptance test")
	}
	t.Parallel()

	h, bucket, vs := blockedStoreHarness(t, "P")
	node := h.nodes[h.nodeIDs[0]]
	v := vs[0]
	h.dropVaultFromConfig(v, true)
	holding, ended, release := holdPurge(bucket, v)
	defer release()
	if err := node.orch.DeleteVault(v.id, true); err != nil {
		t.Fatalf("DeleteVault: %v", err)
	}
	awaitSignal(t, "teardown waiting on the cloud store", holding)

	h.stopNode(node.id)
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the waiting teardown ended with %v, want cancellation by shutdown", err)
		}
	default:
		t.Fatalf("Stop returned while the teardown still waited on the cloud store")
	}
}
