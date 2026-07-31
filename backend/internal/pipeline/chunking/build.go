package chunking

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
)

var (
	// ErrInvalidManifestRef is returned when manifest record numbers are out of order.
	ErrInvalidManifestRef = errors.New("invalid manifest segment ref")
	// ErrMissingSegments is returned when one or more manifest segments are absent locally.
	ErrMissingSegments = errors.New("manifest segments missing locally")
	// ErrAwaitingLocalSegments is returned when this home has not collected every
	// manifest segment yet. Follower homes treat it as a quiet skip; the leader
	// must still surface the underlying missing-segment error.
	ErrAwaitingLocalSegments = errors.New("awaiting local manifest segments")
)

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

// missingManifestSegmentIDs returns segment IDs from manifest refs that are
// not present locally under head/ or completed/.
func missingManifestSegmentIDs(m SealedManifest, locate SegmentLocator) []glid.GLID {
	if locate == nil || len(m.Refs) == 0 {
		return nil
	}
	var missing []glid.GLID
	seen := make(map[glid.GLID]struct{}, len(m.Refs))
	for _, ref := range m.Refs {
		if ref.SegmentID == glid.Nil {
			continue
		}
		if _, ok := seen[ref.SegmentID]; ok {
			continue
		}
		seen[ref.SegmentID] = struct{}{}
		if _, ok := locate.SegmentPath(ref.SegmentID); !ok {
			missing = append(missing, ref.SegmentID)
		}
	}
	return missing
}

// ManifestSpanRefs binds manifest refs to local segment paths. Missing segments
// are returned separately so callers can collect them before GLCB merge.
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

	writeEnd := in.Manifest.SealedAt
	if writeEnd.IsZero() {
		writeEnd = build.Meta.WriteEnd
	}

	return BuildResult{
		GLCBPath:          glcbPath,
		BlobDigest:        build.BlobDigest,
		RecordCount:       build.RecordCount,
		Bytes:             build.Bytes,
		WriteEnd:          writeEnd,
		IngestStart:       build.Meta.IngestStart,
		IngestEnd:         build.Meta.IngestEnd,
		SourceEnd:         build.Meta.SourceEnd,
		IngestTSMonotonic: build.IngestTSMonotonic,
	}, nil
}

// ChunkGLCBPath returns <chunkRoot>/<chunkID>/data.glcb.
func ChunkGLCBPath(chunkRoot string, id chunk.ChunkID) string {
	return filepath.Join(chunkRoot, id.String(), glcb.BlobFilename)
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

// readGLCBSealMeta reads seal metadata from the blob header. Header-only —
// IngestTSMonotonic is persisted in the layout meta at build time, never
// derived by touching record frames: scanning frames for it cost minutes per
// large chunk on slow volumes.
func readGLCBSealMeta(path string) (glcb.BlobMeta, int64, error) {
	blob, err := glcb.OpenMappedBlob(filepath.Clean(path))
	if err != nil {
		return glcb.BlobMeta{}, 0, err
	}
	defer func() { _ = blob.Close() }()

	info, err := os.Stat(filepath.Clean(path))
	if err != nil {
		return glcb.BlobMeta{}, 0, err
	}
	return blob.Meta(), info.Size(), nil
}
