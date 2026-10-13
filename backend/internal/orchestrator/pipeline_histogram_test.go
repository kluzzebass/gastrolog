package orchestrator

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/format"
	"gastrolog/internal/glid"
	filetsidx "gastrolog/internal/index/file/tsidx"
)

// ingestRank resolves ts's ITSI rank in the GLCB at path the way the pipeline
// histogram does.
func ingestRank(path string, ts time.Time) (rank uint32, found bool, err error) {
	err = withGLCBIngestIndex(path, func(mv filetsidx.MmapView) error {
		rank, _, found = mv.SearchTS(ts.UnixNano())
		return nil
	})
	return rank, found, err
}

// An ITSI with no entries yields no rank and no error; an ITSI the TOC does
// not list is missing.
func TestGLCBIngestIndexEmptyIsNotMissing(t *testing.T) {
	t.Parallel()
	base := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	emptyPath, _ := glcbtest.WriteSealedBlob(t, t.TempDir(), chunk.NewChunkID(), glid.New(), nil)
	if _, found, err := ingestRank(emptyPath, base); err != nil || found {
		t.Fatalf("empty ITSI: found=%v err=%v, want found=false err=nil", found, err)
	}

	path, _ := glcbtest.WriteSealedBlob(t, t.TempDir(), chunk.NewChunkID(), glid.New(), glcbtest.Records(3, base, glid.New(), "itsi"))
	if rank, found, err := ingestRank(path, base.Add(time.Millisecond)); err != nil || !found || rank != 1 {
		t.Fatalf("populated ITSI: rank=%d found=%v err=%v, want rank=1 found=true err=nil", rank, found, err)
	}

	data, err := os.ReadFile(path) //nolint:gosec // G304: test-owned temp path
	if err != nil {
		t.Fatal(err)
	}
	const footerSize, entrySize = 44, 42
	count := int(binary.LittleEndian.Uint32(data[len(data)-footerSize:]))
	entries := data[len(data)-footerSize-count*entrySize : len(data)-footerSize]
	for i := range count {
		if entries[i*entrySize] == glcb.SectionIngestTSIndex {
			entries[i*entrySize] = format.TypeTokenIndex
		}
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ingestRank(path, base); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ITSI absent from the TOC: err=%v, want os.ErrNotExist", err)
	}
}
