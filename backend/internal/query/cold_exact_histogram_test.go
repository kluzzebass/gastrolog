package query_test

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
)

// A cold cloud-backed chunk's histogram counts must be EXACT, resolved via
// ranged reads of the object's ITSI section — not spread across buckets in
// proportion to time overlap. The fixture makes the two answers maximally
// different: record density is wildly non-uniform inside the chunk's span,
// which exact rank arithmetic reproduces and overlap smearing flattens.
//
// This is the end-to-end wiring proof on the REAL file manager; the stubbed
// fallback coverage (chunks whose ranks genuinely cannot resolve anywhere)
// lives in cloud_index_missing_histogram_test.go and still stands — archived
// objects and unreachable stores still estimate, visibly.

// vetoingStore fails the test's contract if anything performs a full-object
// download: cold histogram reads are ranged or they are a policy violation.
type vetoingStore struct {
	*blobstore.Memory
	fullGets int
}

func (s *vetoingStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	s.fullGets++
	return s.Memory.Download(ctx, key)
}

func TestColdCloudChunkHistogramIsExact(t *testing.T) {
	t.Parallel()

	const totalRecords = 300
	t0 := time.Date(2026, 5, 4, 14, 0, 0, 0, time.UTC)

	store := &vetoingStore{Memory: blobstore.NewMemory()}
	vaultID := glid.New()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store, VaultID: vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	// 290 records in the first 10 seconds, 10 more spread over the next 90:
	// exact counting puts ~290 in bucket 0 of a ten-bucket window; overlap
	// smearing would put ~30 there. Payloads are incompressible so the
	// object is body-dominated like production chunks.
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic fixture
	appendAt := func(ts time.Time) {
		payload := make([]byte, 256)
		_, _ = rng.Read(payload)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, SourceTS: ts, Raw: payload,
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	for i := range 290 {
		appendAt(t0.Add(time.Duration(i) * 34 * time.Millisecond)) // all inside [t0, t0+10s)
	}
	for i := range 10 {
		appendAt(t0.Add(10*time.Second + time.Duration(i)*9*time.Second)) // one per later bucket
	}

	if err := cm.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range metas {
		if m.Sealed && !m.CloudBacked {
			if err := cm.PostSealProcess(context.Background(), m.ID); err != nil {
				t.Fatalf("post-seal: %v", err)
			}
		}
	}

	// Cold: evict every warm copy (a 1-byte budget evicts everything; a
	// zero budget means eviction disabled), then forbid full downloads.
	if evicted, _ := cm.EvictCacheLRU(1); evicted == 0 {
		t.Fatal("premise: nothing evicted; the chunk never became cold")
	}
	store.fullGets = 0

	im := indexfile.NewManager(dir, nil, nil, cm)
	reg := &testRegistry{
		vaults: map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			vaultID: {cm: cm, im: im},
		},
	}
	eng := query.NewWithRegistry(reg, nil)

	q := query.Query{Start: t0, End: t0.Add(100 * time.Second)}
	buckets := eng.ComputeHistogram(context.Background(), q, 10)
	if len(buckets) != 10 {
		t.Fatalf("got %d buckets, want 10", len(buckets))
	}

	var total int64
	for _, b := range buckets {
		total += b.Count
	}
	if total != totalRecords {
		t.Errorf("histogram total = %d, want exactly %d — estimates round, ranks do not", total, totalRecords)
	}
	// The discriminator: exact ranks reproduce the density spike; overlap
	// smearing flattens it to ~30 per bucket.
	if buckets[0].Count < 250 {
		t.Errorf("bucket 0 = %d, want ≥ 250 (exact would be 290; overlap smearing gives ~30)", buckets[0].Count)
	}
	// Coverage labeling stays: the data IS cloud-backed.
	if !buckets[0].HasCloudData {
		t.Error("bucket 0 lost its cloud-coverage flag")
	}
	if store.fullGets != 0 {
		t.Fatalf("histogram triggered %d full-object downloads; the no-fetch policy is broken", store.fullGets)
	}
}
