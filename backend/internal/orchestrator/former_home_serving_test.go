package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// formerHome is an orchestrator whose config places a vault on another node,
// with the vault's pipeline root on its own file storage: where it kept the
// vault's files while it was a home.
type formerHome struct {
	o     *Orchestrator
	store *sysmem.Store
	vault glid.GLID
	root  string
	// self is the former home's node ID; home is the node the vault is
	// placed on now.
	self, home string
	// storages maps each node ID to its file storage ID.
	storages map[string]string
}

func newFormerHome(t *testing.T) formerHome {
	t.Helper()
	ctx := context.Background()
	store := sysmem.NewStore()
	self, newHome := glid.New(), glid.New()
	storageDir := filepath.Join(t.TempDir(), "storage")
	storages := map[glid.GLID]glid.GLID{}
	for _, node := range []glid.GLID{self, newHome} {
		if err := store.PutNode(ctx, system.NodeConfig{ID: node, Name: node.String()}); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(t.TempDir(), "peer")
		if node == self {
			dir = storageDir
		}
		storages[node] = glid.New()
		if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
			NodeID:       node.String(),
			FileStorages: []system.FileStorage{{ID: storages[node], StorageClass: 1, Path: dir}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	vault := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: vault, Name: "v", Type: system.VaultTypeFile, StorageClass: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetVaultPlacements(ctx, vault, []system.VaultPlacement{
		{StorageID: storages[newHome].String(), Leader: true},
	}); err != nil {
		t.Fatal(err)
	}
	o := newTestOrch(t, Config{LocalNodeID: self.String(), SystemLoader: store})
	if o.isPipelineIngestVault(vault) {
		t.Fatal("premise: the former home is registered with the pipeline for the vault")
	}
	byNode := make(map[string]string, len(storages))
	for node, id := range storages {
		byNode[node.String()] = id.String()
	}
	return formerHome{
		o: o, store: store, vault: vault, root: storagePipelineRoot(storageDir, vault),
		self: self.String(), home: newHome.String(), storages: byNode,
	}
}

func writeFormerHomeFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A node that left a vault's placement, with no pipeline registration for it,
// serves the sealed chunks it kept on the storage it homed the vault on. A
// chunk it does not hold is reported as not existing.
func TestFormerHomeServesGLCBFromItsStorageRoot(t *testing.T) {
	t.Parallel()
	n := newFormerHome(t)
	id := chunk.NewChunkID()
	body := []byte("sealed chunk bytes")
	writeFormerHomeFile(t, chunking.ChunkGLCBPath(pipelineChunkRoot(n.root), id), body)

	var got bytes.Buffer
	if err := n.o.ServeChunkGLCBPull(n.vault, id, &got); err != nil {
		t.Fatalf("ServeChunkGLCBPull from the former home's storage root: %v", err)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Fatalf("served %q, want %q", got.Bytes(), body)
	}
	if err := n.o.ServeChunkGLCBPull(n.vault, chunk.NewChunkID(), &got); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ServeChunkGLCBPull of a chunk not held = %v, want fs.ErrNotExist", err)
	}
}

// The same node serves the segments it collected into head/ while it was a
// home: they are unreleased, so a new home may still need them to build a
// chunk, and their origin may be unreachable. A segment it does not hold is
// reported as unavailable, so the puller moves on to the next holder.
func TestFormerHomeServesHeadSegmentFromItsStorageRoot(t *testing.T) {
	t.Parallel()
	n := newFormerHome(t)
	segID := glid.New()
	body := []byte("segment bytes")
	writeFormerHomeFile(t, paths.HeadSegment(n.root, segID), body)

	var got bytes.Buffer
	if err := n.o.ServeSegmentPull(n.vault, segID, &got); err != nil {
		t.Fatalf("ServeSegmentPull from the former home's head/: %v", err)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Fatalf("served %q, want %q", got.Bytes(), body)
	}
	if err := n.o.ServeSegmentPull(n.vault, glid.New(), &got); !errors.Is(err, collection.ErrSegmentUnavailable) {
		t.Fatalf("ServeSegmentPull of a segment not held = %v, want collection.ErrSegmentUnavailable", err)
	}
}
