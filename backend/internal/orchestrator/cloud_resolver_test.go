package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// These tests pin the production wiring of the lazy cloud-backed resolver
// (gastrolog-5bnxc) across the two cluster edges the retired eager mirrors
// used to cover, using real chunk Managers on both sides of a shared cloud
// store:
//
//   - snapshot install: the follower's FSM is Restore'd wholesale (no
//     per-apply effects fire) and NO projection pass runs — reads must
//     resolve through the lazy resolver alone.
//   - live upload: CmdUploadChunk applies on the follower's FSM with no
//     eager cloud-index registration — the onUpload wiring only emits the
//     inspector event now.

// uploadOneCloudChunk appends records on a leader-side Manager, seals, and
// runs PostSealProcess so the chunk is uploaded to the shared store. Returns
// the chunk ID and its post-upload meta.
func uploadOneCloudChunk(t *testing.T, cm *chunkfile.Manager, records int) (chunk.ChunkID, chunk.ChunkMeta) {
	t.Helper()
	base := time.Date(2025, 7, 1, 12, 0, 0, 0, time.UTC)
	for i := range records {
		ts := base.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "cloud-lazy-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}
	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	var id chunk.ChunkID
	for _, m := range metas {
		if m.Sealed && !m.CloudBacked {
			id = m.ID
			break
		}
	}
	if id == (chunk.ChunkID{}) {
		t.Fatal("no sealed chunk to upload")
	}
	if err := cm.PostSealProcess(context.Background(), id); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}
	meta, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta after upload: %v", err)
	}
	if !meta.CloudBacked {
		t.Fatal("chunk not cloud-backed after PostSealProcess")
	}
	return id, meta
}

// applyCloudChunkLifecycle drives an FSM through Create → BeginSeal → Seal →
// Upload for the given chunk, mirroring what the leader's announcer proposes
// in production.
func applyCloudChunkLifecycle(t *testing.T, fsm *vaultctlfsm.FSM, id chunk.ChunkID, meta chunk.ChunkMeta) {
	t.Helper()
	apply := func(what string, data []byte) {
		t.Helper()
		if err := fsm.Apply(&hraft.Log{Data: data}); err != nil {
			t.Fatalf("apply %s: %v", what, err)
		}
	}
	apply("create", vaultctlfsm.MarshalCreateChunk(id, meta.WriteStart, meta.IngestStart, meta.SourceStart))
	apply("begin-seal", vaultctlfsm.MarshalBeginSeal(id))
	apply("seal", vaultctlfsm.MarshalSealChunk(id, meta.WriteEnd, meta.RecordCount, meta.Bytes,
		meta.IngestStart, meta.IngestEnd, meta.SourceEnd, meta.IngestTSMonotonic, time.Now()))
	apply("upload", vaultctlfsm.MarshalUploadChunk(id, meta.CloudBytes, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0))
}

// newFollowerCloudManager builds a CloudReadOnly Manager sharing the leader's
// store — the production follower shape (buildInstance strips uploads but
// keeps read access). Its cloud index starts empty.
func newFollowerCloudManager(t *testing.T, vaultID glid.GLID, store blobstore.Store) *chunkfile.Manager {
	t.Helper()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        vaultID,
		CloudReadOnly:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	return cm
}

func countCursorRecords(t *testing.T, cm chunk.ChunkManager, id chunk.ChunkID) int {
	t.Helper()
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor(%s): %v", id, err)
	}
	got := 0
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			_ = cursor.Close()
			t.Fatalf("cursor.Next: %v", err)
		}
		got++
	}
	_ = cursor.Close()
	return got
}

// TestLazyCloudResolverSnapshotInstallFollowerServesReads pins the
// snapshot-install edge: the follower's FSM is restored wholesale (no
// per-apply onUpload effects, no projection pass — projectAllCloudBackedFromFSM
// is retired), its cloud index is empty, and OpenCursor still serves the
// chunk's records straight from the shared store via the lazy resolver.
func TestLazyCloudResolverSnapshotInstallFollowerServesReads(t *testing.T) {
	t.Parallel()

	store := blobstore.NewMemory()
	vaultID := glid.New()

	// Leader: upload a chunk and record the FSM lifecycle for it.
	leaderCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = leaderCM.Close() })

	const records = 120
	id, meta := uploadOneCloudChunk(t, leaderCM, records)

	leaderFSM := vaultctlfsm.New()
	applyCloudChunkLifecycle(t, leaderFSM, id, meta)

	// Follower catches up via snapshot install: a wholesale Restore that
	// fires no per-apply effects.
	followerFSM := vaultctlfsm.New()
	followerFSM.RestoreProto(leaderFSM.SnapshotProto())

	followerCM := newFollowerCloudManager(t, vaultID, store)

	// Production wiring: the orchestrator installs the resolver against the
	// vault's ctl group FSM. No projection pass runs anywhere.
	orch, err := New(Config{LocalNodeID: "node-B"})
	if err != nil {
		t.Fatal(err)
	}
	orch.wireLazyCloudBackedResolver(&raftgroup.Group{FSM: followerFSM}, vaultID, followerCM)

	// The follower serves the snapshot-installed chunk on first lookup.
	if got := countCursorRecords(t, followerCM, id); got != records {
		t.Errorf("follower served %d records after snapshot install, want %d", got, records)
	}
	fMeta, err := followerCM.Meta(id)
	if err != nil {
		t.Fatalf("follower Meta: %v", err)
	}
	if !fMeta.CloudBacked || fMeta.RecordCount != records {
		t.Errorf("follower meta = {CloudBacked:%v RecordCount:%d}, want {true %d}",
			fMeta.CloudBacked, fMeta.RecordCount, records)
	}

	// A chunk the FSM never heard of stays a clean miss.
	if _, err := followerCM.OpenCursor(chunk.NewChunkID()); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Errorf("unknown chunk: err = %v, want ErrChunkNotFound", err)
	}
}

// TestLazyCloudResolverLiveUploadFollowerServesReads pins the live-replication
// edge: CmdUploadChunk applies on the follower's FSM with NO eager cloud-index
// registration (the onUpload wiring only emits the inspector event now), and a
// read on the follower resolves lazily from the FSM manifest.
func TestLazyCloudResolverLiveUploadFollowerServesReads(t *testing.T) {
	t.Parallel()

	store := blobstore.NewMemory()
	vaultID := glid.New()

	leaderCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = leaderCM.Close() })

	const records = 80
	id, meta := uploadOneCloudChunk(t, leaderCM, records)

	followerCM := newFollowerCloudManager(t, vaultID, store)
	followerFSM := vaultctlfsm.New()

	orch, err := New(Config{LocalNodeID: "node-B"})
	if err != nil {
		t.Fatal(err)
	}
	g := &raftgroup.Group{FSM: followerFSM}
	// Full production wiring for the upload edge: the event emitter AND the
	// lazy resolver. Neither registers anything eagerly.
	wireVaultFSMOnUpload(g, vaultID, orch)
	orch.wireLazyCloudBackedResolver(g, vaultID, followerCM)

	// Replicate the leader's lifecycle commands — the live path.
	applyCloudChunkLifecycle(t, followerFSM, id, meta)

	if got := countCursorRecords(t, followerCM, id); got != records {
		t.Errorf("follower served %d records after live CmdUploadChunk, want %d", got, records)
	}

	// Enumeration parity: the lister surfaces the FSM-known chunk in List().
	metas, err := followerCM.List()
	if err != nil {
		t.Fatalf("follower List: %v", err)
	}
	var listed bool
	for _, m := range metas {
		if m.ID == id {
			listed = true
			if !m.CloudBacked {
				t.Error("listed follower chunk: CloudBacked = false, want true")
			}
		}
	}
	if !listed {
		t.Error("live-uploaded chunk missing from follower List()")
	}
}
