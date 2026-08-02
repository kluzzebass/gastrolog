package file

import (
	"context"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// A cold cloud-backed chunk — sealed, uploaded, local GLCB evicted — must
// still answer ingest-rank lookups exactly, via ranged reads of the object's
// ITSI section. This is what turns the histogram's estimated counts for cold
// chunks into exact ones. The full-blob no-fetch policy stands: these tests
// fail if a rank lookup triggers a whole-object download.

// gaugedStore wraps the memory store, counting ranged reads and refusing
// full downloads — the policy under test made observable.
type gaugedStore struct {
	*blobstore.Memory
	rangedBytes int64
	rangedGets  int
	fullGets    int
}

func (s *gaugedStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	s.fullGets++
	return s.Memory.Download(ctx, key)
}

func (s *gaugedStore) DownloadRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	s.rangedGets++
	s.rangedBytes += length
	return s.Memory.DownloadRange(ctx, key, off, length)
}

// coldCloudChunk uploads a chunk and evicts its local GLCB, returning the
// chunk ID and the ingest timestamps it was built with.
func coldCloudChunk(t *testing.T, cm *Manager, records int) (chunk.ChunkID, []time.Time) {
	t.Helper()
	base := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	stamps := make([]time.Time, records)
	// Incompressible payloads keep the object dominated by the body frame the
	// way production chunks are; a compressible fixture makes the fixed tail
	// probe look like most of the object and the bytes guard meaningless.
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic fixture
	for i := range records {
		payload := make([]byte, 256)
		_, _ = rng.Read(payload)
		stamps[i] = base.Add(time.Duration(i) * time.Second)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: stamps[i], WriteTS: stamps[i], SourceTS: stamps[i],
			Raw: payload,
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	var id chunk.ChunkID
	for _, m := range metas {
		if m.Sealed && !m.CloudBacked {
			id = m.ID
			if err := cm.PostSealProcess(context.Background(), m.ID); err != nil {
				t.Fatalf("post-seal: %v", err)
			}
			break
		}
	}
	if id == (chunk.ChunkID{}) {
		t.Fatal("no sealed chunk to upload")
	}

	// Make it cold: drop the warm-cache copy the upload left behind.
	cm.evictMappedGLCB(id)
	if err := os.Remove(cm.glcbPath(id)); err != nil {
		t.Fatalf("evict local GLCB: %v", err)
	}
	if cm.HasLocalContent(id) {
		t.Fatal("premise: chunk still reports local content after eviction")
	}
	return id, stamps
}

func TestColdChunkRankResolvesViaRangedReads(t *testing.T) {
	t.Parallel()
	store := &gaugedStore{Memory: blobstore.NewMemory()}
	cm, err := NewManager(Config{
		Dir: t.TempDir(), Now: time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store, VaultID: glid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	id, stamps := coldCloudChunk(t, cm, 300)
	objSize := objectSizeInStore(t, store.Memory)
	store.fullGets = 0 // ignore upload-era traffic; the policy governs reads

	for i, probe := range []time.Time{stamps[0], stamps[150], stamps[299]} {
		rank, ok, err := cm.FindIngestEntryIndex(id, probe)
		if err != nil {
			t.Fatalf("FindIngestEntryIndex(probe %d): %v", i, err)
		}
		if !ok {
			t.Fatalf("probe %d unresolved; cold chunk contributes estimates again", i)
		}
		want := map[int]uint64{0: 0, 1: 150, 2: 299}[i]
		if rank != want {
			t.Errorf("probe %d rank = %d, want %d", i, rank, want)
		}
	}
	// Past-end probes answer not-found, without error.
	if _, ok, err := cm.FindIngestEntryIndex(id, stamps[299].Add(time.Hour)); ok || err != nil {
		t.Errorf("past-end probe = (%v, %v), want (false, nil)", ok, err)
	}

	if store.fullGets != 0 {
		t.Fatalf("rank lookups triggered %d full-blob downloads; the no-fetch policy is broken", store.fullGets)
	}
	if store.rangedGets == 0 {
		t.Fatal("no ranged reads recorded — what answered the lookups?")
	}
	if store.rangedBytes >= objSize/2 {
		t.Errorf("ranged reads fetched %d of %d object bytes; that is not a bounded read", store.rangedBytes, objSize)
	}

	// The section is fetched once and cached: further probes add no GETs.
	gets := store.rangedGets
	if _, _, err := cm.FindIngestEntryIndex(id, stamps[42]); err != nil {
		t.Fatal(err)
	}
	if store.rangedGets != gets {
		t.Errorf("cached lookup issued %d more GETs; every histogram bucket would pay the fetch again",
			store.rangedGets-gets)
	}
}

// An archived object cannot be range-read. The lookup must degrade to
// not-found — the histogram keeps its labeled estimate — never error out of
// the bucket loop.
func TestColdChunkRankOnArchivedObject(t *testing.T) {
	t.Parallel()
	store := &gaugedStore{Memory: blobstore.NewMemory()}
	cm, err := NewManager(Config{
		Dir: t.TempDir(), Now: time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store, VaultID: glid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	id, stamps := coldCloudChunk(t, cm, 20)
	if err := store.Archive(context.Background(), cm.blobKey(id), "GLACIER"); err != nil {
		t.Fatalf("archive: %v", err)
	}

	rank, ok, err := cm.FindIngestEntryIndex(id, stamps[10])
	if err != nil {
		t.Fatalf("archived lookup errored: %v", err)
	}
	if ok {
		t.Fatalf("archived lookup resolved rank %d; what did it read?", rank)
	}
}

// A local-only vault must be untouched by the cold branch.
func TestColdRankBranchIgnoresLocalOnlyVault(t *testing.T) {
	t.Parallel()
	cm, err := NewManager(Config{
		Dir: t.TempDir(), Now: time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		VaultID:        glid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	if _, ok, err := cm.FindIngestEntryIndex(chunk.NewChunkID(), time.Now()); ok || err != nil {
		t.Fatalf("unknown chunk on local-only vault = (%v, %v), want (false, nil)", ok, err)
	}
}

func objectSizeInStore(t *testing.T, store *blobstore.Memory) int64 {
	t.Helper()
	var size int64
	err := store.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		size = info.Size
		return nil
	})
	if err != nil || size == 0 {
		t.Fatalf("object size unavailable (err=%v, size=%d)", err, size)
	}
	return size
}
