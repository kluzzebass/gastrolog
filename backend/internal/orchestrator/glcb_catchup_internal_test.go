package orchestrator

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestHolderPullSourcesExcludesSelfAndEmptyEntries pins the fallback source
// list: every non-empty, non-self node in e.Holders is a candidate; self and
// empty entries are excluded.
func TestHolderPullSourcesExcludesSelfAndEmptyEntries(t *testing.T) {
	t.Parallel()
	o := &Orchestrator{localNodeID: "node-self"}
	e := vaultctlfsm.ManifestEntry{Holders: []string{"node-self", "node-A", "", "node-B"}}

	got := o.holderPullSources(e)
	want := map[string]bool{"node-A": true, "node-B": true}
	if len(got) != len(want) {
		t.Fatalf("holderPullSources = %v, want exactly %v", got, want)
	}
	for _, n := range got {
		if !want[n] {
			t.Errorf("unexpected source %q", n)
		}
	}
}

// TestHolderPullSourcesEmptyWhenNoHolders pins the no-fallback-available
// case: an entry nobody has confirmed yet yields no fallback sources
// either — runGLCBPull's early no-op path stays quiet rather than
// recording a spurious pull attempt/failure.
func TestHolderPullSourcesEmptyWhenNoHolders(t *testing.T) {
	t.Parallel()
	o := &Orchestrator{localNodeID: "node-self"}
	if got := o.holderPullSources(vaultctlfsm.ManifestEntry{}); len(got) != 0 {
		t.Fatalf("holderPullSources on empty Holders = %v, want empty", got)
	}
}

// buildCatchupTestGLCB writes a real segment and builds a valid GLCB from
// it, returning the GLCB path and its record count.
func buildCatchupTestGLCB(t *testing.T, records int) (string, int64) {
	t.Helper()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()

	segPath := filepath.Join(t.TempDir(), segID.String())
	sf, err := segment.Create(segPath, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	for i := range records {
		rec := record.Record{
			EventID: record.EventID{
				IngesterID: glid.New(),
				NodeID:     glid.New(),
				IngestTS:   base.Add(time.Duration(i) * time.Second),
				IngestSeq:  uint32(i),
			},
			SourceTS: base.Add(time.Duration(i) * time.Second),
			IngestTS: base.Add(time.Duration(i) * time.Second),
			Attrs:    record.Attributes{"k": "v"},
			Raw:      []byte("catchup-record"),
		}
		if err := sf.Append(&rec, base); err != nil {
			t.Fatal(err)
		}
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	glcbPath := filepath.Join(t.TempDir(), "data.glcb")
	res, err := chunking.BuildGLCBFile(glcbPath, chunking.BuildGLCBInput{
		ChunkID: chunk.NewChunkID(),
		VaultID: vaultID,
		Refs: []chunking.SpanRef{{
			Path: segPath,
			Span: chunking.Span{SegmentID: segID, Start: 0, Count: uint32(records)},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return glcbPath, int64(res.RecordCount)
}

// copyToTemp copies src into dir as a fresh temp file, standing in for the
// pulled-bytes staging file that verifyAndPromoteGLCB consumes.
func copyToTemp(t *testing.T, src, dir string) string {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(dir, ".glcb.pull.*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func TestVerifyAndPromoteGLCBValid(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	dir := t.TempDir()
	tmp := copyToTemp(t, src, dir)
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: count}
	if err := verifyAndPromoteGLCB(tmp, dest, e, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("verifyAndPromoteGLCB: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("promoted GLCB missing: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file survived promotion (err=%v)", err)
	}
}

func TestVerifyAndPromoteGLCBRecordCountMismatch(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	dir := t.TempDir()
	tmp := copyToTemp(t, src, dir)
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: count + 1}
	if err := verifyAndPromoteGLCB(tmp, dest, e, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("expected record-count mismatch error, got nil")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("mismatched GLCB was promoted (err=%v)", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file not cleaned up after mismatch (err=%v)", err)
	}
}

func TestVerifyAndPromoteGLCBCorruptBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".glcb.pull.corrupt")
	if err := os.WriteFile(tmp, []byte("not a GLCB at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: 1}
	if err := verifyAndPromoteGLCB(tmp, dest, e, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("expected verification error for corrupt blob, got nil")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("corrupt GLCB was promoted (err=%v)", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file not cleaned up after corrupt blob (err=%v)", err)
	}
}

// TestVerifyAndPromoteGLCBTruncated covers the torn-transfer case: a valid
// GLCB cut short mid-file must fail verification, not promote.
func TestVerifyAndPromoteGLCBTruncated(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".glcb.pull.torn")
	if err := os.WriteFile(tmp, data[:len(data)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: count}
	if err := verifyAndPromoteGLCB(tmp, dest, e, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("expected verification error for truncated blob, got nil")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("truncated GLCB was promoted (err=%v)", err)
	}
}

// glcbDigest reads the whole-blob digest a built GLCB carries in its TOC
// footer, which is what the manifest records at upload time.
func glcbDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	toc, err := glcb.ReadTOC(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	return toc.BlobDigest
}

// A peer's bytes are checkable against the manifest, so they are checked.
// Record count alone agrees for any blob carrying the right number of
// records, altered or not.
func TestVerifyAndPromoteGLCBRejectsADigestMismatch(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	dir := t.TempDir()
	tmp := copyToTemp(t, src, dir)
	dest := filepath.Join(dir, "data.glcb")

	// Premise: the blob is otherwise valid, so only the digest can reject
	// it. The manifest claims a different one, as it would for content a
	// peer altered after upload.
	wrong := glcbDigest(t, src)
	wrong[0] ^= 0xff

	e := vaultctlfsm.ManifestEntry{
		ID: chunk.NewChunkID(), RecordCount: count, CloudBacked: true, Hash: wrong,
	}
	err := verifyAndPromoteGLCB(tmp, dest, e, slog.New(slog.DiscardHandler))
	if err == nil {
		t.Fatal("a blob whose digest disagrees with the manifest was promoted")
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("the rejected blob was promoted anyway (err=%v)", statErr)
	}
	if _, statErr := os.Stat(tmp); !os.IsNotExist(statErr) {
		t.Fatalf("the rejected blob was left behind (err=%v)", statErr)
	}
}

func TestVerifyAndPromoteGLCBAcceptsAMatchingDigest(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	dir := t.TempDir()
	tmp := copyToTemp(t, src, dir)
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{
		ID: chunk.NewChunkID(), RecordCount: count, CloudBacked: true, Hash: glcbDigest(t, src),
	}
	if err := verifyAndPromoteGLCB(tmp, dest, e, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("a blob matching the manifest digest was rejected: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("promoted GLCB missing: %v", err)
	}
}

// A chunk that has never been uploaded carries no digest. The pull still
// proceeds, and says which case it is rather than passing over it silently.
func TestVerifyAndPromoteGLCBRecordsAnAbsentDigest(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		cloudBacked bool
		wantLevel   slog.Level
	}{
		{"never uploaded", false, slog.LevelDebug},
		{"cloud-backed with no recorded digest", true, slog.LevelWarn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src, count := buildCatchupTestGLCB(t, 3)
			dir := t.TempDir()
			tmp := copyToTemp(t, src, dir)
			dest := filepath.Join(dir, "data.glcb")

			rec := &levelRecorder{}
			e := vaultctlfsm.ManifestEntry{
				ID: chunk.NewChunkID(), RecordCount: count, CloudBacked: tc.cloudBacked,
			}
			if err := verifyAndPromoteGLCB(tmp, dest, e, slog.New(rec)); err != nil {
				t.Fatalf("verifyAndPromoteGLCB: %v", err)
			}
			if _, err := os.Stat(dest); err != nil {
				t.Fatalf("promoted GLCB missing: %v", err)
			}
			if !rec.sawLevel(tc.wantLevel) {
				t.Fatalf("accepting a blob with no digest recorded nothing at %v (levels seen: %v)",
					tc.wantLevel, rec.levels)
			}
		})
	}
}

// levelRecorder captures the levels a handler was asked to record.
type levelRecorder struct {
	mu     sync.Mutex
	levels []slog.Level
}

func (r *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *levelRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.levels = append(r.levels, rec.Level)
	return nil
}

func (r *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *levelRecorder) WithGroup(string) slog.Handler      { return r }

func (r *levelRecorder) sawLevel(want slog.Level) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.levels {
		if l == want {
			return true
		}
	}
	return false
}
