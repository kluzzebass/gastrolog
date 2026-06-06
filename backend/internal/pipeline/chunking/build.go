package chunking

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
)

var (
	// ErrInvalidManifestRef is returned when manifest record numbers are out of order.
	ErrInvalidManifestRef = errors.New("invalid manifest segment ref")
	// ErrMissingSegments is returned when one or more manifest segments are absent locally.
	ErrMissingSegments = errors.New("manifest segments missing locally")
)

// SegmentLocator resolves on-disk segment paths for one vault home.
type SegmentLocator interface {
	SegmentPath(segmentID glid.GLID) (path string, ok bool)
}

// BuildInput pins inputs for a deterministic GLCB build from a sealed manifest.
type BuildInput struct {
	Manifest  SealedManifest
	VaultID   glid.GLID
	ChunkRoot string
	Locate    SegmentLocator
}

// BuildResult carries the sealed GLCB artifact metadata from BuildSealedChunk.
type BuildResult struct {
	GLCBPath          string
	BlobDigest        [32]byte
	RecordCount       uint32
	Bytes             int64
	WriteEnd          time.Time
	IngestStart       time.Time
	IngestEnd         time.Time
	SourceEnd         time.Time
	IngestTSMonotonic bool
}

// ManifestSpanRefs binds manifest refs to local segment paths. Missing segments
// are returned separately so callers can nudge Collection without guessing paths.
func ManifestSpanRefs(m SealedManifest, locate SegmentLocator) ([]SpanRef, []glid.GLID, error) {
	if locate == nil {
		return nil, nil, errors.New("segment locator required")
	}
	refs := make([]SpanRef, 0, len(m.Refs))
	var missing []glid.GLID
	for _, ref := range m.Refs {
		path, ok := locate.SegmentPath(ref.SegmentID)
		if !ok {
			missing = append(missing, ref.SegmentID)
			continue
		}
		span, err := RefToSpan(ref)
		if err != nil {
			return nil, missing, err
		}
		refs = append(refs, SpanRef{Path: path, Span: span})
	}
	return refs, missing, nil
}

// BuildSealedChunk writes data.glcb for one sealed manifest via MergeSpanRefs + BuildGLCB.
// Re-running with the same inputs is safe: BuildGLCBFile writes through a temp file
// and renames atomically.
func BuildSealedChunk(in BuildInput) (BuildResult, error) {
	if in.Locate == nil {
		return BuildResult{}, errors.New("segment locator required")
	}
	if in.ChunkRoot == "" {
		return BuildResult{}, errors.New("chunk root required")
	}
	spanRefs, missing, err := ManifestSpanRefs(in.Manifest, in.Locate)
	if err != nil {
		return BuildResult{}, err
	}
	if len(missing) > 0 {
		return BuildResult{}, &MissingSegmentsError{SegmentIDs: missing}
	}

	glcbPath := ChunkGLCBPath(in.ChunkRoot, in.Manifest.ChunkID)
	if err := os.MkdirAll(filepath.Dir(glcbPath), 0o750); err != nil {
		return BuildResult{}, fmt.Errorf("create chunk dir: %w", err)
	}

	build, err := BuildGLCBFile(glcbPath, BuildGLCBInput{
		ChunkID: in.Manifest.ChunkID,
		VaultID: in.VaultID,
		Refs:    spanRefs,
	})
	if err != nil {
		return BuildResult{}, err
	}

	meta, monotonic, fileBytes, err := readGLCBSealMeta(glcbPath)
	if err != nil {
		return BuildResult{}, err
	}
	if _, err := os.Stat(glcbPath); err != nil {
		return BuildResult{}, fmt.Errorf("glcb missing after build: %w", err)
	}

	writeEnd := in.Manifest.SealedAt
	if writeEnd.IsZero() {
		writeEnd = meta.WriteEnd
	}

	return BuildResult{
		GLCBPath:          glcbPath,
		BlobDigest:        build.BlobDigest,
		RecordCount:       build.RecordCount,
		Bytes:             fileBytes,
		WriteEnd:          writeEnd,
		IngestStart:       meta.IngestStart,
		IngestEnd:         meta.IngestEnd,
		SourceEnd:         meta.SourceEnd,
		IngestTSMonotonic: monotonic,
	}, nil
}

// ChunkGLCBPath returns <chunkRoot>/<chunkID>/data.glcb.
func ChunkGLCBPath(chunkRoot string, id chunk.ChunkID) string {
	return filepath.Join(chunkRoot, id.String(), chunkcloud.BlobFilename)
}

// MissingSegmentsError lists segment IDs absent from the local head.
type MissingSegmentsError struct {
	SegmentIDs []glid.GLID
}

func (e *MissingSegmentsError) Error() string {
	return fmt.Sprintf("%v: %d segment(s)", ErrMissingSegments, len(e.SegmentIDs))
}

func (e *MissingSegmentsError) Is(target error) bool {
	return target == ErrMissingSegments
}

func readGLCBSealMeta(path string) (chunkcloud.BlobMeta, bool, int64, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return chunkcloud.BlobMeta{}, false, 0, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return chunkcloud.BlobMeta{}, false, 0, err
	}

	rd, err := chunkcloud.NewCacheReader(f)
	if err != nil {
		return chunkcloud.BlobMeta{}, false, 0, err
	}
	defer func() { _ = rd.Close() }()

	meta := rd.Meta()
	monotonic, err := ingestMonotonicInMergeOrder(rd)
	if err != nil {
		return chunkcloud.BlobMeta{}, false, 0, err
	}
	return meta, monotonic, info.Size(), nil
}

func ingestMonotonicInMergeOrder(rd *chunkcloud.Reader) (bool, error) {
	meta := rd.Meta()
	if meta.RecordCount == 0 {
		return true, nil
	}
	var prev time.Time
	for i := range meta.RecordCount {
		rec, err := rd.ReadRecord(i)
		if err != nil {
			return false, err
		}
		if i > 0 && rec.IngestTS.Before(prev) {
			return false, nil
		}
		prev = rec.IngestTS
	}
	return true, nil
}
