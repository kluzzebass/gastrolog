package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// A home's copy of a sealed pipeline chunk is served and uploaded only while it
// is the copy agreed at seal. These tests plant a divergent copy — same chunk,
// other bytes — and replace it the way the landing check and the GLCB catch-up
// pull do: rename aside, then rename the agreed copy onto the path.

const (
	divergentPayload = "divergent"
	agreedPayload    = "agreed!!!"
)

// landCopy builds a sealed GLCB for id whose records carry payload and renames
// it onto path, returning its registration info and whole-blob digest.
func landCopy(t *testing.T, path string, id chunk.ChunkID, vaultID glid.GLID, payload string) (chunk.ExternalGLCBInfo, [32]byte) {
	t.Helper()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	built, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, vaultID,
		glcbtest.Records(20, base, glid.New(), payload))
	digest, err := glcb.ReadBlobDigest(built)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(built, path); err != nil {
		t.Fatal(err)
	}
	return info, digest
}

func (h *pipelineHome) glcbPathFor(id chunk.ChunkID) string {
	return filepath.Join(h.chunkRoot, id.String(), dataGLCBFileName)
}

// quarantine renames the copy on path aside, as the landing check does with a
// divergent copy.
func quarantine(t *testing.T, path string) {
	t.Helper()
	if err := os.Rename(path, path+".divergent"); err != nil {
		t.Fatal(err)
	}
}

func assertPayloads(t *testing.T, raws [][]byte, want string) {
	t.Helper()
	if len(raws) == 0 {
		t.Fatal("read served no records")
	}
	for i, raw := range raws {
		if string(raw) != want {
			t.Fatalf("record %d payload = %q, want %q", i, raw, want)
		}
	}
}

// A query that mapped the divergent copy holds that mapping while the copy is
// quarantined and the agreed copy lands on the same path. Its next read must
// map the agreed copy, not reuse the mapping keyed by the unchanged path.
func TestMappedGLCB_ReplacedCopyIsNotServedFromTheCachedMapping(t *testing.T) {
	t.Parallel()
	h := newPipelineHome(t, blobstore.NewMemory(), glid.New(), "", 0, 0, nil)
	id := chunk.NewChunkID()
	path := h.glcbPathFor(id)
	info, _ := landCopy(t, path, id, h.cm.cfg.VaultID, divergentPayload)
	if err := h.cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatal(err)
	}

	held, err := h.cm.OpenCursor(id)
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := held.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Raw) != divergentPayload {
		t.Fatalf("premise: the held cursor reads the divergent copy, got %q", rec.Raw)
	}

	quarantine(t, path)
	landCopy(t, path, id, h.cm.cfg.VaultID, agreedPayload)
	_ = held.Close()

	assertPayloads(t, drainCursorRaw(t, h.cm, id), agreedPayload)
}

// The same replacement seen through a section read, which shares the cached
// mapping with cursors.
func TestMappedGLCB_SectionReadFollowsTheReplacedCopy(t *testing.T) {
	t.Parallel()
	h := newPipelineHome(t, blobstore.NewMemory(), glid.New(), "", 0, 0, nil)
	id := chunk.NewChunkID()
	path := h.glcbPathFor(id)
	info, divergent := landCopy(t, path, id, h.cm.cfg.VaultID, divergentPayload)
	if err := h.cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatal(err)
	}
	digestNow := func() [32]byte {
		t.Helper()
		blob, err := h.cm.mappedGLCB(id)
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Release()
		return blob.BlobDigest()
	}
	if got := digestNow(); got != divergent {
		t.Fatal("premise: the first mapping is of the divergent copy")
	}

	quarantine(t, path)
	_, agreed := landCopy(t, path, id, h.cm.cfg.VaultID, agreedPayload)

	if got := digestNow(); got != agreed {
		t.Fatalf("mapping digest = %x after the agreed copy landed, want %x", got[:8], agreed[:8])
	}
}

// A home registered its own copy before another home's upload made the chunk
// cloud-backed. Once that copy is quarantined, forgetting it routes the next
// read to the cloud object instead of a registration naming a path that no
// longer holds the copy.
func TestForgetPipelineGLCB_QuarantinedCopyReadsTheCloudObject(t *testing.T) {
	t.Parallel()
	shared := blobstore.NewMemory()
	vaultID := glid.New()
	uploader := newPipelineHome(t, shared, vaultID, "", 0, 0, nil)
	home := newPipelineHome(t, shared, vaultID, "", 0, 0, nil)
	manifest := newFakeManifest()
	manifest.install(home.cm)

	id := chunk.NewChunkID()
	agreedInfo, _ := landCopy(t, uploader.glcbPathFor(id), id, vaultID, agreedPayload)
	if err := uploader.cm.RegisterExternalGLCB(id, uploader.glcbPathFor(id), agreedInfo); err != nil {
		t.Fatal(err)
	}

	path := home.glcbPathFor(id)
	info, _ := landCopy(t, path, id, vaultID, divergentPayload)
	if err := home.cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatal(err)
	}
	assertPayloads(t, drainCursorRaw(t, home.cm, id), divergentPayload)

	if err := uploader.cm.UploadToCloud(id); err != nil {
		t.Fatalf("UploadToCloud: %v", err)
	}
	manifest.markUploaded(id, agreedInfo, 1)

	quarantine(t, path)
	home.cm.ForgetPipelineGLCB(id)

	assertPayloads(t, drainCursorRaw(t, home.cm, id), agreedPayload)
	if home.store.downloads.Load() != 1 {
		t.Errorf("cloud downloads = %d, want 1 (the read of the agreed object)", home.store.downloads.Load())
	}
}

// sealedDigests is the sealed digest per chunk, as the vault-ctl FSM records
// it at seal.
type sealedDigests map[chunk.ChunkID][32]byte

func (v sealedDigests) ExpectedDigest(id chunk.ChunkID) ([32]byte, bool) {
	d, ok := v[id]
	return d, ok
}

// uploadRecorder records every upload announce.
type uploadRecorder struct {
	chunk.MetadataAnnouncer
	mu      sync.Mutex
	digests map[chunk.ChunkID][32]byte
}

func (r *uploadRecorder) AnnounceUpload(id chunk.ChunkID, _, _, _, _, _ int64, hash [32]byte, _ glid.GLID, _ uint8) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.digests == nil {
		r.digests = map[chunk.ChunkID][32]byte{}
	}
	r.digests[id] = hash
}

func (r *uploadRecorder) announced(id chunk.ChunkID) ([32]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.digests[id]
	return d, ok
}

// storedDigest downloads the chunk's cloud object and returns the whole-blob
// digest its bytes hash to.
func storedDigest(t *testing.T, h *pipelineHome, id chunk.ChunkID) [32]byte {
	t.Helper()
	dst, err := os.Create(filepath.Join(t.TempDir(), "fetched.glcb"))
	if err != nil {
		t.Fatal(err)
	}
	if err := glcb.DownloadAndUnwrap(context.Background(), h.store, h.cm.blobKey(id), dst); err != nil {
		t.Fatalf("download: %v", err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := glcb.VerifyBlobDigest(dst.Name())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func objectExists(h *pipelineHome, id chunk.ChunkID) bool {
	info, err := h.store.Head(context.Background(), h.cm.blobKey(id))
	return err == nil && info.Size > 0
}

// verifiedHome is a pipeline home whose uploads are checked against sealed
// digests and whose announces are recorded.
func verifiedHome(t *testing.T, store blobstore.Store, vaultID glid.GLID, sealed sealedDigests) (*pipelineHome, *uploadRecorder) {
	t.Helper()
	h := newPipelineHome(t, store, vaultID, "", 0, 0, nil)
	rec := &uploadRecorder{}
	h.cm.SetAnnouncer(rec)
	h.cm.SetIntegrityVerifier(sealed)
	return h, rec
}

// A home whose copy is divergent uploads nothing — not in the window before
// its landing check, and not later — and announces nothing. Once the agreed
// copy lands on the path, the same home uploads the agreed bytes.
func TestUploadToCloud_DivergentCopyNeverReachesTheStore(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	id := chunk.NewChunkID()
	sealed := sealedDigests{}
	h, rec := verifiedHome(t, blobstore.NewMemory(), vaultID, sealed)

	agreedSrc := filepath.Join(t.TempDir(), "agreed.glcb")
	agreedInfo, agreed := landCopy(t, agreedSrc, id, vaultID, agreedPayload)
	sealed[id] = agreed

	path := h.glcbPathFor(id)
	info, divergent := landCopy(t, path, id, vaultID, divergentPayload)
	if divergent == agreed {
		t.Fatal("premise: the two copies differ")
	}
	if err := h.cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatal(err)
	}

	err := h.cm.UploadToCloud(id)
	if !errors.Is(err, ErrGLCBDisagreesWithSeal) {
		t.Fatalf("UploadToCloud of a divergent copy = %v, want ErrGLCBDisagreesWithSeal", err)
	}
	if objectExists(h, id) {
		t.Fatal("the divergent copy reached the cloud store")
	}
	if d, ok := rec.announced(id); ok {
		t.Fatalf("a divergent copy's upload was announced with digest %x", d[:8])
	}

	quarantine(t, path)
	h.cm.ForgetPipelineGLCB(id)
	if err := os.Rename(agreedSrc, path); err != nil {
		t.Fatal(err)
	}
	// What the lazy resolver does on the next lookup of the landed copy.
	if err := h.cm.RegisterExternalGLCB(id, path, agreedInfo); err != nil {
		t.Fatal(err)
	}
	if err := h.cm.UploadToCloud(id); err != nil {
		t.Fatalf("UploadToCloud of the agreed copy: %v", err)
	}
	if got := storedDigest(t, h, id); got != agreed {
		t.Fatalf("stored object digest = %x, want the agreed %x", got[:8], agreed[:8])
	}
	if d, _ := rec.announced(id); d != agreed {
		t.Fatalf("announced digest = %x, want the agreed %x", d[:8], agreed[:8])
	}
}

// An object another home already uploaded is not adopted by a home whose own
// copy is divergent: adopting would announce that copy and keep it as the
// cloud-backed chunk's warm cache.
func TestUploadToCloud_DivergentCopyNeverAdoptsTheStoredObject(t *testing.T) {
	t.Parallel()
	shared := blobstore.NewMemory()
	vaultID := glid.New()
	id := chunk.NewChunkID()
	sealed := sealedDigests{}
	uploader, _ := verifiedHome(t, shared, vaultID, sealed)
	home, rec := verifiedHome(t, shared, vaultID, sealed)

	agreedInfo, agreed := landCopy(t, uploader.glcbPathFor(id), id, vaultID, agreedPayload)
	sealed[id] = agreed
	if err := uploader.cm.RegisterExternalGLCB(id, uploader.glcbPathFor(id), agreedInfo); err != nil {
		t.Fatal(err)
	}
	if err := uploader.cm.UploadToCloud(id); err != nil {
		t.Fatalf("uploader UploadToCloud: %v", err)
	}

	info, _ := landCopy(t, home.glcbPathFor(id), id, vaultID, divergentPayload)
	if err := home.cm.RegisterExternalGLCB(id, home.glcbPathFor(id), info); err != nil {
		t.Fatal(err)
	}
	if err := home.cm.UploadToCloud(id); !errors.Is(err, ErrGLCBDisagreesWithSeal) {
		t.Fatalf("UploadToCloud of a divergent copy over a stored object = %v, want ErrGLCBDisagreesWithSeal", err)
	}
	if d, ok := rec.announced(id); ok {
		t.Fatalf("a divergent copy adopted the stored object, announcing digest %x", d[:8])
	}
	if meta, err := home.cm.Meta(id); err != nil || meta.CloudBacked {
		t.Fatalf("divergent home's meta = %+v, %v; want its copy left local, not adopted", meta, err)
	}
	if got := storedDigest(t, home, id); got != agreed {
		t.Fatalf("stored object digest = %x, want the agreed %x", got[:8], agreed[:8])
	}
}

// The upload announce carries the uploaded copy's whole-blob digest, read from
// its TOC footer — for an agreed copy, and for a chunk whose seal recorded no
// digest, where the announce is what records one.
func TestUploadToCloud_AnnouncesTheCopysDigest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		sealedDigest bool
	}{
		{"sealed digest recorded", true},
		{"no sealed digest", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vaultID := glid.New()
			id := chunk.NewChunkID()
			sealed := sealedDigests{}
			h, rec := verifiedHome(t, blobstore.NewMemory(), vaultID, sealed)
			path := h.glcbPathFor(id)
			info, digest := landCopy(t, path, id, vaultID, agreedPayload)
			if tc.sealedDigest {
				sealed[id] = digest
			}
			if err := h.cm.RegisterExternalGLCB(id, path, info); err != nil {
				t.Fatal(err)
			}
			if err := h.cm.UploadToCloud(id); err != nil {
				t.Fatalf("UploadToCloud: %v", err)
			}
			got, ok := rec.announced(id)
			if !ok {
				t.Fatal("upload not announced")
			}
			if got != digest {
				t.Fatalf("announced digest = %x, want the copy's %x", got[:8], digest[:8])
			}
			if stored := storedDigest(t, h, id); stored != digest {
				t.Fatalf("stored object digest = %x, want %x", stored[:8], digest[:8])
			}
		})
	}
}
