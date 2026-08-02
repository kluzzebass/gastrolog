package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// The audit grounds its EXPECTATION in the FSM manifest, because that is what
// owns which chunks are cloud-backed. These tests drive that seam directly and
// manipulate the blob store underneath it, which is how each divergence
// category is produced in isolation — several of them coincide in the real
// world and would otherwise be impossible to tell apart in an assertion.

// auditFixture stands up a cloud-backed vault with n uploaded chunks and wires
// the FSM seam to claim exactly those chunks, so the starting point is a vault
// the audit should call clean.
type auditFixture struct {
	orch    *Orchestrator
	vaultID glid.GLID
	ids     []chunk.ChunkID
	store   *blobstoreMemory
	// entries is what the FSM seam reports; tests mutate it to model cluster
	// state diverging from the store.
	entries      []vaultctlfsm.ManifestEntry
	tombstoned   map[chunk.ChunkID]bool
	blobKeyOf    func(chunk.ChunkID) string
	cloudBytesOf map[chunk.ChunkID]int64
}

func TestAuditCloudIndexCleanVault(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 2)

	audit, err := f.orch.AuditVaultCloudIndex(context.Background(), f.vaultID)
	if err != nil {
		t.Fatalf("AuditVaultCloudIndex: %v", err)
	}
	if audit.ExpectedChunks != 2 || audit.StoreObjects != 2 || audit.IndexEntries != 2 {
		t.Errorf("counts = expected %d / store %d / index %d, want 2/2/2",
			audit.ExpectedChunks, audit.StoreObjects, audit.IndexEntries)
	}
	if !audit.Clean() {
		t.Errorf("healthy vault reports divergence: %+v", audit)
	}
}

// The cardinal-rule case: the cluster believes the chunk is durable and the
// bytes are gone. Nothing else detects this — no event fires when a provider
// lifecycle rule or an operator deletes an object.
func TestAuditCloudIndexReportsMissingBlob(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 2)
	f.deleteBlob(t, f.ids[0])

	audit := f.audit(t)
	if len(audit.MissingBlobs) != 1 || audit.MissingBlobs[0] != f.ids[0] {
		t.Fatalf("MissingBlobs = %v, want [%s]", audit.MissingBlobs, f.ids[0])
	}
	if audit.Clean() {
		t.Error("audit reports clean with a chunk's bytes missing")
	}
}

func TestAuditCloudIndexReportsSizeMismatch(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	// The FSM recorded a size the object does not have. In production this is
	// a truncated or re-written object, or an upload that recorded the wrong
	// transport size.
	f.setExpectedCloudBytes(f.ids[0], f.cloudBytesOf[f.ids[0]]+512)

	audit := f.audit(t)
	if len(audit.SizeMismatches) != 1 {
		t.Fatalf("SizeMismatches = %+v, want 1 entry", audit.SizeMismatches)
	}
	got := audit.SizeMismatches[0]
	if got.ID != f.ids[0] || got.ExpectedBytes <= got.StoreBytes {
		t.Errorf("mismatch = %+v, want ID %s with ExpectedBytes > StoreBytes", got, f.ids[0])
	}
}

// Bytes in the store that no chunk claims: the operator is paying for them and
// nothing will ever read them.
func TestAuditCloudIndexReportsUntrackedBlob(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	stray := f.uploadStrayBlob(t)

	audit := f.audit(t)
	if len(audit.UntrackedBlobs) != 1 || audit.UntrackedBlobs[0] != stray {
		t.Fatalf("UntrackedBlobs = %v, want [%s]", audit.UntrackedBlobs, stray)
	}
	if len(audit.MissingBlobs) != 0 {
		t.Errorf("an extra object was also counted as missing: %v", audit.MissingBlobs)
	}
}

// A tombstoned chunk's object is explainable — the delete is in flight — so it
// must not read as leaked bytes an operator should investigate.
func TestAuditCloudIndexSeparatesTombstonedFromUntracked(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	stray := f.uploadStrayBlob(t)
	f.tombstoned[stray] = true

	audit := f.audit(t)
	if len(audit.UntrackedBlobs) != 0 {
		t.Errorf("tombstoned object counted as untracked: %v", audit.UntrackedBlobs)
	}
	if len(audit.TombstonedBlobs) != 1 || audit.TombstonedBlobs[0] != stray {
		t.Fatalf("TombstonedBlobs = %v, want [%s]", audit.TombstonedBlobs, stray)
	}
}

// Cache drift, both directions. These are node-local repair cases rather than
// durability ones, and the audit must not conflate them with the FSM-grounded
// categories above.
func TestAuditCloudIndexReportsStaleCacheEntry(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	id := f.ids[0]
	// Store and cluster agree the chunk is gone; only this node's cache
	// still believes it exists.
	f.deleteBlob(t, id)
	f.entries = nil

	audit := f.audit(t)
	if len(audit.StaleIndexEntries) != 1 || audit.StaleIndexEntries[0] != id {
		t.Fatalf("StaleIndexEntries = %v, want [%s]", audit.StaleIndexEntries, id)
	}
	if len(audit.MissingBlobs) != 0 {
		t.Errorf("a chunk the cluster no longer claims was reported missing: %v", audit.MissingBlobs)
	}
}

func TestAuditCloudIndexReportsUnindexedBlob(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	// An object the cluster knows about that this node's cache has never
	// seen — the shape a cache rebuild fixes.
	extra := f.uploadStrayBlob(t)
	f.expectChunk(extra, 128)

	audit := f.audit(t)
	if len(audit.UnindexedBlobs) != 1 || audit.UnindexedBlobs[0] != extra {
		t.Fatalf("UnindexedBlobs = %v, want [%s]", audit.UnindexedBlobs, extra)
	}
	if len(audit.UntrackedBlobs) != 0 {
		t.Errorf("a cluster-known object was reported untracked: %v", audit.UntrackedBlobs)
	}
}

// A chunk the cluster deleted must never be reported as data loss. Its object
// being gone IS the delete succeeding, and calling that a missing blob would
// raise a durability alarm for work GastroLog did on purpose — the exact false
// alarm the reconcile sweep's tombstone exemption existed to prevent.
func TestAuditCloudIndexDoesNotReportTombstonedChunkAsMissing(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 2)
	deleted := f.ids[0]

	// Retention's shape: the object is gone and the cluster has tombstoned the
	// chunk, while the manifest entry has not been finalized away yet.
	f.deleteBlob(t, deleted)
	f.tombstoned[deleted] = true

	audit := f.audit(t)
	for _, id := range audit.MissingBlobs {
		if id == deleted {
			t.Errorf("tombstoned chunk %s reported as a missing blob", deleted)
		}
	}
	if audit.ExpectedChunks != 1 {
		t.Errorf("ExpectedChunks = %d, want 1 — a deleted chunk is not expected to exist", audit.ExpectedChunks)
	}
}

// A local-only vault must be distinguishable from a cloud vault with nothing in
// it; "0 objects, all clean" would be a lie an operator could act on.
func TestAuditCloudIndexOnLocalOnlyVault(t *testing.T) {
	t.Parallel()
	orch, vaultID := newLocalOnlyVaultForAudit(t)

	_, err := orch.AuditVaultCloudIndex(context.Background(), vaultID)
	if !errors.Is(err, chunk.ErrCloudStoreNotConfigured) {
		t.Fatalf("err = %v, want ErrCloudStoreNotConfigured", err)
	}
}

func TestAuditCloudIndexUnknownVault(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)

	if _, err := f.orch.AuditVaultCloudIndex(context.Background(), glid.New()); err == nil {
		t.Fatal("auditing a vault this node does not home returned no error")
	}
}

// A vault whose objects are all archived is still auditable: archived is a
// storage-class fact, not an absence, and reporting it as missing would send an
// operator chasing data loss that has not happened.
func TestAuditCloudIndexArchivedObjectsAreNotMissing(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	f.archiveAll(t)

	audit := f.audit(t)
	if len(audit.MissingBlobs) != 0 {
		t.Errorf("archived object reported missing: %v", audit.MissingBlobs)
	}
	if audit.ArchivedObjects != 1 {
		t.Errorf("ArchivedObjects = %d, want 1", audit.ArchivedObjects)
	}
}

func (f *auditFixture) audit(t *testing.T) CloudIndexAudit {
	t.Helper()
	audit, err := f.orch.AuditVaultCloudIndex(context.Background(), f.vaultID)
	if err != nil {
		t.Fatalf("AuditVaultCloudIndex: %v", err)
	}
	return audit
}

func (f *auditFixture) deleteBlob(t *testing.T, id chunk.ChunkID) {
	t.Helper()
	if err := f.store.Delete(context.Background(), f.blobKeyOf(id)); err != nil {
		t.Fatalf("delete blob %s: %v", id, err)
	}
}

func (f *auditFixture) uploadStrayBlob(t *testing.T) chunk.ChunkID {
	t.Helper()
	id := chunk.NewChunkID()
	if err := f.store.Upload(context.Background(), f.blobKeyOf(id),
		strings.NewReader("bytes no chunk claims"), nil); err != nil {
		t.Fatalf("upload stray blob: %v", err)
	}
	return id
}

func (f *auditFixture) expectChunk(id chunk.ChunkID, cloudBytes int64) {
	f.entries = append(f.entries, vaultctlfsm.ManifestEntry{
		ID: id, State: chunk.ChunkStateSealed,
		CloudBacked: true, CloudBytes: cloudBytes,
		WriteStart: time.Now(),
	})
}

func (f *auditFixture) setExpectedCloudBytes(id chunk.ChunkID, n int64) {
	for i := range f.entries {
		if f.entries[i].ID == id {
			f.entries[i].CloudBytes = n
		}
	}
}
