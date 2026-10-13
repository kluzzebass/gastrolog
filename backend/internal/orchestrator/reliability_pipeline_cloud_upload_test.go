package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// cloudPutLedger is one bucket seen through per-node views. It records, per
// chunk, every PUT each node attempted and every PUT that stored the object,
// and can hold a node's Head or Upload calls until the test releases them.
type cloudPutLedger struct {
	shared blobstore.Store

	mu       sync.Mutex
	labels   map[string]string // node ID -> label
	attempts map[string][]string
	stored   map[string][]string
	holds    map[string]*cloudCallHold // "<op>:<nodeID>" -> hold
}

// cloudCallHold parks a node's calls of one operation until released; once
// released, parked and later calls end with outcome (nil lets them proceed).
type cloudCallHold struct {
	entered chan string // chunk ID of each parked call
	done    chan struct{}
	outcome error
}

var errHeldHeadNotFound = errors.New("held head: object not found")

func newCloudPutLedger() *cloudPutLedger {
	return &cloudPutLedger{
		shared:   blobstore.NewMemory(),
		attempts: map[string][]string{},
		stored:   map[string][]string{},
		holds:    map[string]*cloudCallHold{},
		labels:   map[string]string{},
	}
}

func (l *cloudPutLedger) view(nodeID string) blobstore.Store {
	return &cloudPutView{Store: l.shared, ledger: l, node: nodeID}
}

func (l *cloudPutLedger) nameNodes(h *orchRelHarness) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, id := range h.nodeIDs {
		l.labels[id] = h.nodes[id].label
	}
}

// hold parks the node's calls of op from now on: "head", "upload" (a PUT
// before it stores the object) or "stored" (a PUT after it stored it).
func (l *cloudPutLedger) hold(op, nodeID string) *cloudCallHold {
	hd := &cloudCallHold{entered: make(chan string, 64), done: make(chan struct{})}
	l.mu.Lock()
	l.holds[op+":"+nodeID] = hd
	l.mu.Unlock()
	return hd
}

// release ends every parked call with outcome. lift stops holding new calls;
// without it every later call of op on the node also ends with outcome.
func (l *cloudPutLedger) release(op, nodeID string, outcome error, lift bool) {
	l.mu.Lock()
	hd := l.holds[op+":"+nodeID]
	if lift {
		delete(l.holds, op+":"+nodeID)
	}
	l.mu.Unlock()
	if hd == nil {
		return
	}
	hd.outcome = outcome
	close(hd.done)
}

// await parks one call of op on the node when it is held, and returns the
// outcome the call ends with.
func (l *cloudPutLedger) await(op, nodeID, chunkID string) error {
	l.mu.Lock()
	hd := l.holds[op+":"+nodeID]
	l.mu.Unlock()
	if hd == nil {
		return nil
	}
	select {
	case <-hd.done:
	default:
		hd.entered <- chunkID
		<-hd.done
	}
	return hd.outcome
}

func (l *cloudPutLedger) label(nodeID string) string {
	if s, ok := l.labels[nodeID]; ok {
		return s
	}
	return nodeID
}

// putsFor returns (attempting nodes, storing nodes) for a chunk.
func (l *cloudPutLedger) putsFor(id chunk.ChunkID) (attempts, stored []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.attempts[id.String()]), slices.Clone(l.stored[id.String()])
}

func (l *cloudPutLedger) totalStored() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, nodes := range l.stored {
		n += len(nodes)
	}
	return n
}

func (l *cloudPutLedger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := make([]string, 0, len(l.attempts))
	for k := range l.attempts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "  %s attempts=%v stored=%v\n", k, l.attempts[k], l.stored[k])
	}
	return b.String()
}

type cloudPutView struct {
	blobstore.Store
	ledger *cloudPutLedger
	node   string
}

func chunkIDOfKey(key string) string {
	return strings.TrimSuffix(path.Base(key), ".glcb")
}

func (v *cloudPutView) Head(ctx context.Context, key string) (blobstore.BlobInfo, error) {
	if err := v.ledger.await("head", v.node, chunkIDOfKey(key)); err != nil {
		return blobstore.BlobInfo{}, err
	}
	return v.Store.Head(ctx, key)
}

func (v *cloudPutView) Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error {
	id := chunkIDOfKey(key)
	v.ledger.mu.Lock()
	v.ledger.attempts[id] = append(v.ledger.attempts[id], v.ledger.label(v.node))
	v.ledger.mu.Unlock()
	if err := v.ledger.await("upload", v.node, id); err != nil {
		return err
	}
	if err := v.Store.Upload(ctx, key, data, metadata); err != nil {
		return err
	}
	v.ledger.mu.Lock()
	v.ledger.stored[id] = append(v.ledger.stored[id], v.ledger.label(v.node))
	v.ledger.mu.Unlock()
	return v.ledger.await("stored", v.node, id)
}

// cloudUploadFailures counts, per node, the cloud-upload jobs that failed —
// an upload attempted before the node's copy existed fails "chunk not found".
type cloudUploadFailures struct {
	mu     sync.Mutex
	failed map[string][]string
}

func watchCloudUploadFailures(t *testing.T, h *orchRelHarness) *cloudUploadFailures {
	t.Helper()
	w := &cloudUploadFailures{failed: map[string][]string{}}
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		sub, cancel := n.orch.Scheduler().Events().Subscribe()
		t.Cleanup(cancel)
		label := n.label
		go func() {
			for evt := range sub.Events() {
				if evt.Kind != orchestrator.JobEventFailed || !strings.HasPrefix(evt.Job.Name, "cloud-upload:") {
					continue
				}
				w.mu.Lock()
				w.failed[label] = append(w.failed[label], evt.Job.Name)
				w.mu.Unlock()
			}
		}()
	}
	return w
}

func (w *cloudUploadFailures) assertNone(t *testing.T) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.failed) != 0 {
		t.Fatalf("cloud-upload jobs failed: %v", w.failed)
	}
}

func newCloudUploadHarness(t *testing.T, n int) (*orchRelHarness, *cloudPutLedger) {
	t.Helper()
	ledger := newCloudPutLedger()
	h := newOrchRelHarness(t, n,
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
		withNodeCloudStores(ledger.view),
	)
	ledger.nameNodes(h)
	return h, ledger
}

// waitCloudBacked waits until the vault-ctl FSM, observed on a node, has every
// sealed chunk of the vault cloud-backed and holds at least want of them.
func (h *orchRelHarness) waitCloudBacked(v vaultSpec, observer string, want int, ledger *cloudPutLedger) []vaultctlfsm.ManifestEntry {
	h.t.Helper()
	var entries []vaultctlfsm.ManifestEntry
	h.waitProgress(fmt.Sprintf("vault %s: %d sealed chunks cloud-backed", v.label, want),
		100*time.Millisecond, func() (string, bool) {
			entries = h.sealedPipelineChunks(v, observer)
			backed := 0
			for _, e := range entries {
				if e.CloudBacked {
					backed++
				}
			}
			return fmt.Sprintf("sealed=%d cloud_backed=%d stored_puts=%d", len(entries), backed, ledger.totalStored()),
				len(entries) >= want && backed == len(entries)
		}, func() {
			h.dumpPipelineState(v)
			h.t.Logf("cloud PUT ledger:\n%s", ledger)
		})
	return entries
}

// waitCloudUploadsSettled waits until no live node has a cloud-upload job
// registered: every upload any node scheduled has run to its end.
func (h *orchRelHarness) waitCloudUploadsSettled(v vaultSpec) {
	h.t.Helper()
	h.waitCloudUploadsSettledExcept(v, "")
}

// waitCloudUploadsSettledExcept is waitCloudUploadsSettled for every live
// node but skip.
func (h *orchRelHarness) waitCloudUploadsSettledExcept(v vaultSpec, skip string) {
	h.t.Helper()
	prefix := "cloud-upload:" + v.id.String() + ":"
	h.waitProgress("no cloud-upload job pending", 50*time.Millisecond, func() (string, bool) {
		var pending []string
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			if id != skip && n.orch != nil && n.orch.Scheduler().HasPendingPrefix(prefix) {
				pending = append(pending, n.label)
			}
		}
		return fmt.Sprintf("pending=%v", pending), len(pending) == 0
	}, nil)
}

// assertUploadedOnceBy asserts each chunk was PUT exactly once, by uploader,
// and that the object is in the bucket.
func assertUploadedOnceBy(t *testing.T, ledger *cloudPutLedger, entries []vaultctlfsm.ManifestEntry, uploader string) {
	t.Helper()
	for _, e := range entries {
		attempts, stored := ledger.putsFor(e.ID)
		if len(attempts) != 1 || len(stored) != 1 || stored[0] != uploader {
			t.Fatalf("chunk %s: PUT attempts=%v stored=%v, want exactly one PUT by %s\nledger:\n%s",
				e.ID, attempts, stored, uploader, ledger)
		}
	}
}

func newEntries(all []vaultctlfsm.ManifestEntry, seen map[chunk.ChunkID]bool) []vaultctlfsm.ManifestEntry {
	var out []vaultctlfsm.ManifestEntry
	for _, e := range all {
		if !seen[e.ID] {
			seen[e.ID] = true
			out = append(out, e)
		}
	}
	return out
}

// rePlace writes a new placement for the vault (first index is the placement
// leader) and drives the dispatcher-equivalent reconcile on every live node.
func (h *orchRelHarness) rePlace(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	ctx := context.Background()
	h.setVaultPlacements(v, nodeIdxs)
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if n.orch == nil {
			continue
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			h.t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
		n.orch.ReconcileVaultPlacement(ctx, v.id)
	}
}

// uploadWithLeaderHeld ingests chunks' worth of records on ingest while the
// placement leader's existence checks are held, waits until every home holds
// every sealed chunk and every other node's upload jobs have run, then lets
// the leader proceed and waits for every chunk to be cloud-backed. A home that
// uploads anyway finds no object while the leader is held, so its upload
// shows as a PUT of its own.
func (h *orchRelHarness) uploadWithLeaderHeld(v vaultSpec, ledger *cloudPutLedger, ingest string, chunks int, prefix string) []vaultctlfsm.ManifestEntry {
	h.t.Helper()
	leader := h.nodeIDs[v.nodeIdxs[0]]
	ledger.hold("head", leader)
	h.submitIngestRecords(ingest, chunks*pipelineChunkMaxRecords, prefix)
	sealed := h.waitSealedRecords(v, leader, int64(chunks*pipelineChunkMaxRecords))
	h.waitHeldByHomes(v, leader, sealed, v.nodeIdxs)
	h.waitCloudUploadsSettledExcept(v, leader)
	ledger.release("head", leader, nil, true)

	entries := h.waitCloudBacked(v, leader, chunks, ledger)
	h.waitCloudUploadsSettled(v)
	return entries
}

// Steady state: every sealed chunk of a cloud-backed vault reaches the bucket
// through exactly one PUT, made by the placement leader, and no node attempts
// an upload before its own copy exists. Single-node and 4-node clusters.
func TestOrchPipeline_CloudUploadExactlyOncePerChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	for _, tc := range []struct {
		nodes int
		homes []int
	}{
		{nodes: 1, homes: []int{0}},
		{nodes: 4, homes: []int{0, 1, 2}},
	} {
		t.Run(fmt.Sprintf("%d-nodes", tc.nodes), func(t *testing.T) {
			h, ledger := newCloudUploadHarness(t, tc.nodes)
			failures := watchCloudUploadFailures(t, h)
			v := h.addCloudCacheVault("C", tc.homes, "")

			entries := h.uploadWithLeaderHeld(v, ledger, h.nodeIDs[tc.nodes-1], 4, "once")

			assertUploadedOnceBy(t, ledger, entries, h.nodes[h.nodeIDs[0]].label)
			failures.assertNone(t)
		})
	}
}

// A node that built the vault before any placement existed has no claim to
// upload: the placement that arrives later names the uploader, and only it
// PUTs.
func TestOrchPipeline_CloudUploadNodeBuiltBeforePlacement(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	h, ledger := newCloudUploadHarness(t, 4)
	failures := watchCloudUploadFailures(t, h)

	v := h.putCloudVault("C", "")
	h.addVaultOnEveryNode(v)
	v.nodeIdxs = []int{0, 1, 2}
	h.vaults = append(h.vaults, v)
	h.rePlace(v, v.nodeIdxs)

	entries := h.uploadWithLeaderHeld(v, ledger, h.nodeIDs[3], 3, "pre-placement")

	assertUploadedOnceBy(t, ledger, entries, h.nodes[h.nodeIDs[0]].label)
	failures.assertNone(t)
}

// waitHeldByHomes waits until the vault-ctl FSM, observed on a node, records a
// holder receipt from every home for every entry. A home schedules a chunk's
// cloud upload, when it owes one, before it claims the receipt.
func (h *orchRelHarness) waitHeldByHomes(v vaultSpec, observer string, entries []vaultctlfsm.ManifestEntry, homeIdxs []int) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: every home holds every chunk", v.label), 100*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, observer)
		if sub == nil {
			return "no vault-ctl FSM", false
		}
		held := 0
		for _, want := range entries {
			e := sub.Get(want.ID)
			if e == nil {
				continue
			}
			for _, idx := range homeIdxs {
				if slices.Contains(e.Holders, h.nodeIDs[idx]) {
					held++
				}
			}
		}
		return fmt.Sprintf("receipts=%d/%d", held, len(entries)*len(homeIdxs)), held == len(entries)*len(homeIdxs)
	}, func() { h.dumpPipelineState(v) })
}

// The placement leader moves mid-stream from node-1 to node-2, a follower
// when its vault instance was built. Uploads node-1 still had queued when it
// lost the role are left to node-2 — not duplicated — and node-2, promoted,
// uploads both those and every chunk sealed after the move: each chunk is
// PUT exactly once.
func TestOrchPipeline_CloudUploadAcrossPlacementLeaderChange(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	h, ledger := newCloudUploadHarness(t, 4)
	v := h.addCloudCacheVault("C", []int{0, 1, 2}, "")
	ingest := h.nodeIDs[3]
	oldLeader, newLeader := h.nodeIDs[0], h.nodeIDs[1]
	seen := map[chunk.ChunkID]bool{}

	h.submitIngestRecords(ingest, 2*pipelineChunkMaxRecords, "before")
	before := newEntries(h.waitCloudBacked(v, oldLeader, 2, ledger), seen)
	h.waitCloudUploadsSettled(v)
	assertUploadedOnceBy(t, ledger, before, h.nodes[oldLeader].label)

	// node-1's next uploads park on their existence check, with the role
	// still node-1's.
	parked := ledger.hold("head", oldLeader)
	h.submitIngestRecords(ingest, 2*pipelineChunkMaxRecords, "during")
	h.waitSealedRecords(v, oldLeader, 4*pipelineChunkMaxRecords)
	waitParked(t, parked, 2)

	h.rePlace(v, []int{1, 0, 2})
	during := newEntries(h.waitCloudBacked(v, newLeader, 4, ledger), seen)

	// The parked uploads resume as if the object were still missing: the
	// role, not the object, must stop them.
	ledger.release("head", oldLeader, errHeldHeadNotFound, true)
	h.waitCloudUploadsSettled(v)
	assertUploadedOnceBy(t, ledger, during, h.nodes[newLeader].label)

	h.submitIngestRecords(ingest, 2*pipelineChunkMaxRecords, "after")
	after := newEntries(h.waitCloudBacked(v, newLeader, 6, ledger), seen)
	h.waitCloudUploadsSettled(v)
	assertUploadedOnceBy(t, ledger, after, h.nodes[newLeader].label)
	assertUploadedOnceBy(t, ledger, before, h.nodes[oldLeader].label)
}

// The uploader dies mid-upload, and the vault is re-placed onto the surviving
// homes. Dying before its PUT stored the object, it leaves no object, and the
// new placement leader PUTs the chunk exactly once. Dying after the object was
// stored but before the upload committed, it leaves the object, and the new
// leader adopts it without a PUT of its own. Either way the bucket ends with
// exactly one stored object and the chunk cloud-backed.
func TestOrchPipeline_CloudUploaderDiesMidUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	for _, tc := range []struct {
		name          string
		diesAt        string // the ledger hold the uploader dies on
		successorPUTs int
		storedByDying bool
	}{
		{name: "before-the-object-is-stored", diesAt: "upload", successorPUTs: 1},
		{name: "after-the-object-is-stored", diesAt: "stored", successorPUTs: 0, storedByDying: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, ledger := newCloudUploadHarness(t, 4)
			v := h.addCloudCacheVault("C", []int{0, 1, 2}, "")
			ingest := h.nodeIDs[3]
			dying, successor := h.nodeIDs[0], h.nodeIDs[1]
			seen := map[chunk.ChunkID]bool{}

			h.submitIngestRecords(ingest, pipelineChunkMaxRecords, "before")
			before := newEntries(h.waitCloudBacked(v, dying, 1, ledger), seen)
			h.waitCloudUploadsSettled(v)

			inFlight := ledger.hold(tc.diesAt, dying)
			h.submitIngestRecords(ingest, pipelineChunkMaxRecords, "dying")
			victim := waitParked(t, inFlight, 1)[0]

			ledger.release(tc.diesAt, dying, errors.New("connection reset: uploader died"), false)
			h.stopNode(dying)
			h.rePlace(v, []int{1, 2})

			died := newEntries(h.waitCloudBacked(v, successor, 2, ledger), seen)
			if len(died) != 1 || died[0].ID.String() != victim {
				t.Fatalf("chunks sealed after the first = %v, want the in-flight chunk %s", died, victim)
			}
			h.waitCloudUploadsSettled(v)

			attempts, stored := ledger.putsFor(died[0].ID)
			successorPUTs := 0
			for _, node := range attempts {
				if node == h.nodes[successor].label {
					successorPUTs++
				}
			}
			wantStored := []string{h.nodes[successor].label}
			if tc.storedByDying {
				wantStored = []string{h.nodes[dying].label}
			}
			if successorPUTs != tc.successorPUTs || !slices.Equal(stored, wantStored) {
				t.Fatalf("chunk %s: PUT attempts=%v stored=%v, want %d PUT(s) by %s and stored=%v\nledger:\n%s",
					died[0].ID, attempts, stored, tc.successorPUTs, h.nodes[successor].label, wantStored, ledger)
			}
			key := "vault-" + v.id.String() + "/" + died[0].ID.String() + ".glcb"
			if _, err := ledger.shared.Head(context.Background(), key); err != nil {
				t.Fatalf("object %s missing from the bucket: %v", key, err)
			}
			assertUploadedOnceBy(t, ledger, before, h.nodes[dying].label)
		})
	}
}

// waitParked waits for n calls to park on a hold and returns their chunk IDs.
func waitParked(t *testing.T, hd *cloudCallHold, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for len(ids) < n {
		select {
		case id := <-hd.entered:
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		case <-time.After(orchHarnessEventTimeout):
			t.Fatalf("%d of %d calls parked: %v", len(ids), n, ids)
		}
	}
	return ids
}
