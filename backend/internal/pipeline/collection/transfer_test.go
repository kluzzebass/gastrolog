package collection_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

func writeSegmentBytes(t *testing.T, vaultID, segID glid.GLID, raw string) []byte {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, segID.String())

	sf, err := segment.Create(path, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC)
	rec := &record.Record{
		SourceTS: ts,
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   ts,
			IngestSeq:  0,
		},
		Attrs: record.Attributes{"k": "v"},
		Raw:   []byte(raw),
	}
	if err := sf.Append(rec, ts); err != nil {
		t.Fatal(err)
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
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReceiveAndPromoteVerified(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	segID := glid.New()
	data := writeSegmentBytes(t, vaultID, segID, "verified payload")

	prePath, err := collection.ReceiveToPreHead(root, segID, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	headPath, _, err := collection.PromoteVerified(prePath, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head file: %v", err)
	}
	if _, err := os.Stat(prePath); !os.IsNotExist(err) {
		t.Fatal("pre-head copy should be gone after promote")
	}
	if _, err := os.Stat(paths.PreHeadSegment(root, segID)); !os.IsNotExist(err) {
		t.Fatal("pre-head should be empty after promote")
	}
	sf, err := segment.Open(headPath)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := sf.ReadAll()
	_ = sf.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || string(recs[0].Raw) != "verified payload" {
		t.Fatalf("records = %+v", recs)
	}
}

func TestPromoteVerifiedRejectsCorruptTransfer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	if err := paths.EnsurePreHeadDir(root); err != nil {
		t.Fatal(err)
	}
	prePath := paths.PreHeadSegment(root, segID)
	if err := os.WriteFile(prePath, []byte("not a segment"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err := collection.PromoteVerified(prePath, root)
	if !errors.Is(err, collection.ErrCorruptSegment) {
		t.Fatalf("PromoteVerified() = %v, want ErrCorruptSegment", err)
	}
	if _, err := os.Stat(prePath); !os.IsNotExist(err) {
		t.Fatal("corrupt pre-head file should be removed")
	}
	head, err := os.ReadDir(paths.HeadDir(root))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(head) != 0 {
		t.Fatal("head must stay empty when verification fails")
	}
}

func TestReceiveToPreHeadCopyError(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	_, err := collection.ReceiveToPreHead(root, segID, &failReader{err: errors.New("transfer interrupted")})
	if err == nil {
		t.Fatal("expected copy error")
	}
	if _, err := os.Stat(paths.PreHeadSegment(root, segID)); !os.IsNotExist(err) {
		t.Fatal("partial pre-head file should be removed")
	}
}

type failReader struct{ err error }

func (r *failReader) Read([]byte) (int, error) { return 0, r.err }

func TestPreHeadDoesNotSatisfyHeadInvariant(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	segID := glid.New()
	data := writeSegmentBytes(t, vaultID, segID, "still in pre-head")

	if _, err := collection.ReceiveToPreHead(root, segID, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	headEntries, err := os.ReadDir(paths.HeadDir(root))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(headEntries) != 0 {
		t.Fatal("segment in pre-head must not appear in head")
	}
}

type stubPull struct {
	data []byte
	err  error
}

func (p stubPull) Pull(_ context.Context, _, _ glid.GLID, dest io.Writer) error {
	if p.err != nil {
		return p.err
	}
	_, err := io.Copy(dest, bytes.NewReader(p.data))
	return err
}

func TestPullToPreHeadStreamsWithoutBuffer(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	data := writeSegmentBytes(t, vaultID, segID, "streamed segment")
	root := t.TempDir()

	path, err := collection.PullToPreHead(context.Background(), root, vaultID, segID, stubPull{data: data})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("pre-head bytes mismatch: got %d want %d", len(got), len(data))
	}
}

// localFirstPull mimics segmentPullClient: read local layout before remote.
type localFirstPull struct {
	root   string
	seg    glid.GLID
	remote stubPull
}

func (p localFirstPull) Pull(_ context.Context, _, segmentID glid.GLID, dest io.Writer) error {
	for _, path := range []string{
		paths.PreHeadSegment(p.root, segmentID),
	} {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			_, werr := dest.Write(data)
			return werr
		}
	}
	return p.remote.Pull(context.Background(), glid.New(), segmentID, dest)
}

func TestPullToPreHeadDoesNotCopyEmptyPreHeadPlaceholder(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	data := writeSegmentBytes(t, vaultID, segID, "remote segment")
	root := t.TempDir()

	path, err := collection.PullToPreHead(context.Background(), root, vaultID, segID, localFirstPull{
		root:   root,
		seg:    segID,
		remote: stubPull{data: data},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("pre-head bytes mismatch: got %d want %d", len(got), len(data))
	}
}
