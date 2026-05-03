package query_test

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	"gastrolog/internal/manifest"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

// indexMissingCM wraps a ChunkManager so that List()/Meta() report
// CloudBacked=true and FindIngestEntryIndex returns (0, false). This
// reproduces the production setup where a cloud-backed chunk is in the
// FSM manifest but the local IngestTS rank index isn't cached on this
// node — search can still stream the records from the blob, but the
// histogram path can't resolve per-bucket ranks.
type indexMissingCM struct {
	chunk.ChunkManager
}

func (c *indexMissingCM) List() ([]chunk.ChunkMeta, error) {
	metas, err := c.ChunkManager.List()
	if err != nil {
		return nil, err
	}
	for i := range metas {
		metas[i].CloudBacked = true
	}
	return metas, nil
}

func (c *indexMissingCM) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m, err := c.ChunkManager.Meta(id)
	if err != nil {
		return m, err
	}
	m.CloudBacked = true
	return m, nil
}

// FindIngestEntryIndex always reports "not found" — the local index file
// is missing on this node.
func (c *indexMissingCM) FindIngestEntryIndex(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}

// FindIngestStartPosition similarly fails.
func (c *indexMissingCM) FindIngestStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}

// indexMissingIM wraps an IndexManager whose lookups all report
// ErrIndexNotFound, mirroring a node that hasn't fetched the index file.
type indexMissingIM struct {
	index.IndexManager
}

func (im *indexMissingIM) FindIngestEntryIndex(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}

func (im *indexMissingIM) FindIngestStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}

// TestCloudIndexMissingHistogramIncludesAllRecords reproduces the
// production gap: 1800+ cloud chunks each with 100 records exist in the
// FSM, but timechartChunkByIndex's rank-arithmetic path can't resolve
// per-bucket ranks because the local cache is empty. The histogram total
// must still equal RecordCount × chunks (within distribution rounding).
func TestCloudIndexMissingHistogramIncludesAllRecords(t *testing.T) {
	t.Parallel()

	const numChunks = 100
	const recordsPerChunk = 50
	totalRecords := numChunks * recordsPerChunk

	t0 := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)

	vaultID := glid.New()
	v := memtest.MustNewVault(t, chunkmem.Config{
		// Seal each chunk after recordsPerChunk records.
		RotationPolicy: chunk.NewRecordCountPolicy(recordsPerChunk),
	})
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Second)
		v.CM.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "rec-%d", i),
		})
	}
	v.CM.Seal() // seal final active chunk

	reg := &testRegistry{
		vaults: map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			vaultID: {
				cm: &indexMissingCM{v.CM},
				im: &indexMissingIM{v.IM},
			},
		},
	}

	eng := query.NewWithRegistry(reg, nil)

	// Span the full data range; 50 buckets matches the UI default.
	q := query.Query{
		Start: t0,
		End:   t0.Add(time.Duration(totalRecords) * time.Second),
	}

	buckets := eng.ComputeHistogram(context.Background(), q, 50)
	var got int64
	for _, b := range buckets {
		got += b.Count
	}

	if errors.Is(nil, nil) {
		t.Logf("buckets=%d, total=%d, want=%d", len(buckets), got, totalRecords)
	}
	// Allow modest rounding loss from proportional distribution but the
	// total must NOT collapse to the ~50% the production cluster sees.
	minAcceptable := int64(totalRecords) * 95 / 100
	if got < minAcceptable {
		t.Errorf("histogram total = %d, want at least %d (records present in FSM but missing from histogram)", got, minAcceptable)
	}
}

// quietTestRegistry wraps testRegistry to silence the registry wiring noise.
// Reuse manifest.NewProjectingReader so the FSM path projects from cm.List().
var _ = manifest.NewProjectingReader
