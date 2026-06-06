package chunking

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
	"gastrolog/internal/record"
)

var ErrNoMergeRecords = errors.New("merge produced no records")

// BuildGLCBInput names the chunk identity and span plan for a GLCB build.
type BuildGLCBInput struct {
	ChunkID chunk.ChunkID
	VaultID glid.GLID
	Refs    []SpanRef
}

// BuildGLCBResult carries the sealed chunk metadata produced by BuildGLCB.
type BuildGLCBResult struct {
	RecordCount uint32
	BlobDigest  [32]byte
}

// BuildGLCB encodes merged span records into GLCB (design-notes §37).
// Records stream to disk during the merge; only dictionary and index metadata
// stay in memory. EventID and raw bytes are copied verbatim; attributes are
// normalized into the chunk string dictionary.
func BuildGLCB(dst io.Writer, in BuildGLCBInput) (BuildGLCBResult, error) {
	f, ok := dst.(*os.File)
	if !ok {
		tmp, err := os.CreateTemp("", "glcb-out-*.tmp")
		if err != nil {
			return BuildGLCBResult{}, err
		}
		tmpPath := tmp.Name()
		defer func() { _ = os.Remove(tmpPath) }()
		result, err := buildGLCBFile(tmp, in)
		if err != nil {
			_ = tmp.Close()
			return BuildGLCBResult{}, err
		}
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			_ = tmp.Close()
			return BuildGLCBResult{}, err
		}
		if _, err := io.Copy(dst, tmp); err != nil {
			_ = tmp.Close()
			return BuildGLCBResult{}, err
		}
		_ = tmp.Close()
		return result, nil
	}
	return buildGLCBFile(f, in)
}

// BuildGLCBFile writes a GLCB to path atomically via a temp file and rename.
func BuildGLCBFile(path string, in BuildGLCBInput) (BuildGLCBResult, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".glcb.tmp.*")
	if err != nil {
		return BuildGLCBResult{}, err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	result, err := buildGLCBFile(tmp, in)
	if err != nil {
		_ = tmp.Close()
		return BuildGLCBResult{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return BuildGLCBResult{}, err
	}
	if err := tmp.Close(); err != nil {
		return BuildGLCBResult{}, err
	}
	dest := filepath.Clean(path)
	if err := os.Rename(tmpPath, dest); err != nil { //nolint:gosec // G703: path from pipeline caller, not untrusted input
		return BuildGLCBResult{}, err
	}
	cleanup = false
	return result, nil
}

func buildGLCBFile(f *os.File, in BuildGLCBInput) (BuildGLCBResult, error) {
	w, err := chunkcloud.OpenWriter(f, in.ChunkID, in.VaultID)
	if err != nil {
		return BuildGLCBResult{}, err
	}

	var recordCount uint32
	for rec, err := range MergeSpanRefs(in.Refs) {
		if err != nil {
			return BuildGLCBResult{}, err
		}
		if err := w.Add(toChunkRecord(rec)); err != nil {
			return BuildGLCBResult{}, fmt.Errorf("encode record: %w", err)
		}
		recordCount++
	}
	if recordCount == 0 {
		return BuildGLCBResult{}, ErrNoMergeRecords
	}

	toc, err := w.Finish()
	if err != nil {
		return BuildGLCBResult{}, fmt.Errorf("finish GLCB: %w", err)
	}
	return BuildGLCBResult{
		RecordCount: recordCount,
		BlobDigest:  toc.BlobDigest,
	}, nil
}

func toChunkRecord(rec record.Record) chunk.Record {
	return chunk.Record{
		SourceTS: rec.SourceTS,
		IngestTS: rec.IngestTS,
		WriteTS:  rec.WriteTS,
		EventID: chunk.EventID{
			IngesterID: rec.EventID.IngesterID,
			NodeID:     rec.EventID.NodeID,
			IngestTS:   rec.EventID.IngestTS,
			IngestSeq:  rec.EventID.IngestSeq,
		},
		Attrs: toChunkAttrs(rec.Attrs),
		Raw:   rec.Raw,
	}
}

func toChunkAttrs(attrs record.Attributes) chunk.Attributes {
	if len(attrs) == 0 {
		return nil
	}
	return chunk.Attributes(attrs)
}
