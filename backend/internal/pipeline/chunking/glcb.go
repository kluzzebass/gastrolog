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
// dst must be an *os.File opened in the directory where the blob will live
// (use BuildGLCBFile for the common atomic-rename path). EventID and raw bytes
// are copied verbatim; attributes are normalized into the chunk string dictionary.
func BuildGLCB(dst io.Writer, in BuildGLCBInput) (BuildGLCBResult, error) {
	workDir, err := glcbWorkDir(dst)
	if err != nil {
		return BuildGLCBResult{}, err
	}
	return buildGLCBTo(dst, workDir, in)
}

func glcbWorkDir(dst io.Writer) (string, error) {
	f, ok := dst.(*os.File)
	if !ok {
		return "", errors.New("BuildGLCB requires a file in its final directory; use BuildGLCBFile")
	}
	return chunkcloud.WorkDirForFile(f)
}

// BuildGLCBFile writes a GLCB to path atomically via a temp file and rename.
// Record staging and the output temp both use filepath.Dir(path) so the build
// never crosses filesystems.
func BuildGLCBFile(path string, in BuildGLCBInput) (BuildGLCBResult, error) {
	workDir := filepath.Dir(path)
	tmp, err := os.CreateTemp(workDir, ".glcb.tmp.*")
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

	result, err := buildGLCBTo(tmp, workDir, in)
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

func buildGLCBTo(dst io.Writer, workDir string, in BuildGLCBInput) (BuildGLCBResult, error) {
	w, err := chunkcloud.NewWriter(in.ChunkID, in.VaultID, workDir)
	if err != nil {
		return BuildGLCBResult{}, err
	}
	defer func() { _ = w.Close() }()

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

	if _, err := w.WriteTo(dst); err != nil {
		return BuildGLCBResult{}, fmt.Errorf("write GLCB: %w", err)
	}
	toc := w.TOC()
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
