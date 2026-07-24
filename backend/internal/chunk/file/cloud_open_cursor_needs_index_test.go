package file

import (
	"errors"
	"testing"

	"gastrolog/internal/chunk"
)

// TestOpenCursorCloudBackedRequiresCloudIndex is the verification pin for
// gastrolog-5bnxc: it establishes, empirically, that OpenCursor for a
// cloud-backed chunk whose blob lives only in the cloud store (no local
// data.glcb warm cache) resolves SOLELY through the local cloud index — there
// is no FSM-grounded / lazy substitute in the chunk Manager for that byte-access
// path.
//
// Why this matters for the retirement question: projectAllCloudBackedFromFSM
// (reconciler, snapshot install) and the vault-ctl FSM onUpload effect (live
// CmdUploadChunk replication) both exist to call RegisterCloudBackedChunk, which
// populates this cloud index. Snapshot install replaces the FSM's chunk set
// wholesale and fires NO per-apply onUpload effect, so a follower that catches
// up via snapshot has the cloud entry in its FSM but nothing in its cloud index.
// The Manager's lazy on-miss resolver (SetExternalGLCBResolver) does NOT close
// that gap: it serves only pipeline GLCBs that are physically present on disk
// (it os.Stat's the local file), so an evicted / never-downloaded cloud blob
// stays unresolvable through it. This test reproduces exactly that state and
// shows OpenCursor fails until the cloud index is (re)populated — which is why
// the eager projection / onUpload registration must remain.
func TestOpenCursorCloudBackedRequiresCloudIndex(t *testing.T) {
	t.Parallel()

	cm := newEvictionTestManager(t, "lru", 0, 0, nil)

	const records = 50
	ids := uploadN(t, cm, 1, records)
	if len(ids) != 1 {
		t.Fatalf("expected 1 cloud-backed chunk, got %d", len(ids))
	}
	id := ids[0]

	// Capture the cloud-backed metadata the FSM would carry for this chunk,
	// then reproduce the snapshot-install follower state: blob in the cloud
	// store, no local warm cache, and NO cloud-index entry (a wholesale FSM
	// Restore fires no per-apply onUpload effect).
	cm.cloudIdxMu.Lock()
	cmeta, ok := cm.cloudIdx.Lookup(id)
	cm.cloudIdxMu.Unlock()
	if !ok || cmeta == nil {
		t.Fatalf("fixture: chunk %s not in cloud index after upload", id)
	}
	info := chunk.CloudBackedChunkInfo{
		WriteStart:        cmeta.writeStart,
		WriteEnd:          cmeta.writeEnd,
		IngestStart:       cmeta.ingestStart,
		IngestEnd:         cmeta.ingestEnd,
		SourceStart:       cmeta.sourceStart,
		SourceEnd:         cmeta.sourceEnd,
		RecordCount:       cmeta.recordCount,
		Bytes:             cmeta.bytes,
		CloudBytes:        cmeta.cloudBytes,
		IngestIdxOffset:   cmeta.ingestIdxOffset,
		IngestIdxSize:     cmeta.ingestIdxSize,
		SourceIdxOffset:   cmeta.sourceIdxOffset,
		SourceIdxSize:     cmeta.sourceIdxSize,
		IngestTSMonotonic: cmeta.ingestTSMonotonic,
	}

	// Evict the warm cache (removes the local data.glcb) ...
	if evicted, _ := cm.EvictCacheLRU(1); evicted != 1 {
		t.Fatalf("EvictCacheLRU: evicted = %d, want 1", evicted)
	}
	// ... and drop the cloud-index entry + any local meta, so the chunk is
	// present in neither m.metas, the cloud index, nor a local GLCB.
	cm.cloudIdxMu.Lock()
	if _, err := cm.cloudIdx.Delete(id); err != nil {
		t.Fatalf("cloudIdx.Delete: %v", err)
	}
	if err := cm.cloudIdx.Sync(); err != nil {
		t.Fatalf("cloudIdx.Sync: %v", err)
	}
	cm.cloudIdxMu.Unlock()
	cm.mu.Lock()
	delete(cm.metas, id)
	cm.cloudListCache = nil
	cm.mu.Unlock()

	// With the entry gone from every local tier, OpenCursor cannot resolve the
	// chunk — the lazy/grounded path does not cover a cloud-only blob. This is
	// the gap projectAllCloudBackedFromFSM / onUpload exist to prevent.
	if _, err := cm.OpenCursor(id); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("OpenCursor before cloud-index registration: err = %v, want ErrChunkNotFound "+
			"(if this now resolves, a lazy cloud resolver exists and the eager projection may be retireable)", err)
	}

	// RegisterCloudBackedChunk is exactly the per-entry action of both
	// projectAllCloudBackedFromFSM and the onUpload effect. After it, the chunk
	// resolves and OpenCursor serves its records straight from the cloud store.
	if err := cm.RegisterCloudBackedChunk(id, info); err != nil {
		t.Fatalf("RegisterCloudBackedChunk: %v", err)
	}

	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor after cloud-index registration: %v", err)
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
	if got != records {
		t.Errorf("served %d records from the cloud after registration, want %d", got, records)
	}
}
