package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// blobstoreMemory names the concrete in-memory store the fixture hands back, so
// tests can reach past the Store interface to Delete/Archive out of band —
// which is exactly what the audit exists to detect.
type blobstoreMemory = blobstore.Memory

// newAuditFixture stands up a cloud-backed vault, uploads n chunks, and wires
// the FSM seam to claim exactly those chunks as cloud-backed with the sizes the
// store actually holds. The result is a vault the audit must call clean; each
// test then breaks one thing.
func newAuditFixture(t *testing.T, n int) *auditFixture {
	t.Helper()
	vaultID := glid.New()
	csID := glid.New()
	cloudStore := blobstore.NewMemory()
	dir := t.TempDir()

	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore: cloudStore, VaultID: vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	im := indexfile.NewManager(dir, nil, nil, cm)

	store := sysmem.NewStore()
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "cloud-index-audit", Type: system.VaultTypeFile, CloudServiceID: &csID,
	})
	_ = store.PutCloudService(context.Background(), system.CloudService{
		ID: csID, Name: "audit-cloud", Provider: "memory", ArchivalMode: "active",
	})

	orch := newTestOrch(t, Config{
		LocalNodeID:  "audit-node",
		SystemLoader: &transitionSystemLoader{store: store},
	})

	f := &auditFixture{
		orch:         orch,
		vaultID:      vaultID,
		store:        cloudStore,
		tombstoned:   map[chunk.ChunkID]bool{},
		cloudBytesOf: map[chunk.ChunkID]int64{},
	}

	vaultInst := &VaultInstance{
		VaultID: vaultID, Type: "cloud",
		Chunks: cm, Indexes: im, Query: query.New(cm, im, nil),
		ManifestReadFacet: ManifestReadFacet{
			ManifestEntries: func() []vaultctlfsm.ManifestEntry { return f.entries },
			IsTombstoned:    func(id chunk.ChunkID) bool { return f.tombstoned[id] },
		},
	}
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	for range n {
		id, _ := uploadOneCloudChunk(t, cm, 5)
		f.ids = append(f.ids, id)
	}
	// Read the sizes back from the store so the fixture's "expected" values are
	// the store's own truth: a fixture that guessed them would report a size
	// mismatch on every run.
	blobs, err := cm.ListCloudBlobs(context.Background())
	if err != nil {
		t.Fatalf("fixture: ListCloudBlobs: %v", err)
	}
	for _, b := range blobs {
		f.cloudBytesOf[b.ID] = b.Size
		f.expectChunk(b.ID, b.Size)
	}
	f.blobKeyOf = func(id chunk.ChunkID) string {
		return "vault-" + vaultID.String() + "/" + id.String() + ".glcb"
	}
	if len(f.ids) != n {
		t.Fatalf("fixture: uploaded %d chunks, want %d", len(f.ids), n)
	}
	return f
}

// reuploadBlob puts an object back under a chunk's key, modelling a re-upload
// or a provider restore completing after the sweep saw the chunk as missing.
func (f *auditFixture) reuploadBlob(t *testing.T, id chunk.ChunkID) {
	t.Helper()
	if err := f.store.Upload(context.Background(), f.blobKeyOf(id),
		strings.NewReader("the object is back"), nil); err != nil {
		t.Fatalf("reupload blob %s: %v", id, err)
	}
}

// archiveAll transitions every object in the store to an offline class, the
// state an operator's lifecycle rules produce.
func (f *auditFixture) archiveAll(t *testing.T) {
	t.Helper()
	err := f.store.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		return f.store.Archive(context.Background(), info.Key, "GLACIER")
	})
	if err != nil {
		t.Fatalf("archive all: %v", err)
	}
}

// newLocalOnlyVaultForAudit registers a vault with no cloud store at all.
func newLocalOnlyVaultForAudit(t *testing.T) (*Orchestrator, glid.GLID) {
	t.Helper()
	vaultID := glid.New()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000),
		VaultID: vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	store := sysmem.NewStore()
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "local-only", Type: system.VaultTypeFile,
	})
	orch := newTestOrch(t, Config{
		LocalNodeID:  "audit-node",
		SystemLoader: &transitionSystemLoader{store: store},
	})
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{
		VaultID: vaultID, Type: "file", Chunks: cm,
	}))
	return orch, vaultID
}
