package glcb

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// buildDirectBlobFile writes a small GLCB through the writer's direct path —
// records streamed into the output file, layout patched at the end — the path
// pipeline builds take. buildBlobFile covers the staging path.
func buildDirectBlobFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.glcb")
	f, err := os.Create(path) //nolint:gosec // test temp path
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(chunk.NewChunkID(), glid.New(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.BindOutput(f); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := range uint32(3) {
		if err := w.Add(chunk.Record{
			SourceTS: now.Add(-time.Second),
			IngestTS: now,
			WriteTS:  now.Add(time.Duration(i) * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: glid.New(), IngestTS: now, IngestSeq: i + 1},
			Attrs:    chunk.Attributes{"host": "web-1"},
			Raw:      []byte("digest test record"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.WriteTo(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// The footer digest is what CmdSealChunk records and every home compares, so
// both writer paths must produce the digest of every byte before the footer.
func TestBlobDigestCoversEveryByteBeforeTheFooter(t *testing.T) {
	t.Parallel()
	staged, _ := buildBlobFile(t)
	for name, path := range map[string]string{"staging": staged, "direct": buildDirectBlobFile(t)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			recorded, err := ReadBlobDigest(path)
			if err != nil {
				t.Fatalf("ReadBlobDigest: %v", err)
			}
			verified, err := VerifyBlobDigest(path)
			if err != nil {
				t.Fatalf("VerifyBlobDigest on an untouched blob: %v", err)
			}
			if verified != recorded {
				t.Fatalf("verified digest %x != footer digest %x", verified[:8], recorded[:8])
			}
			blob, err := OpenMappedBlob(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = blob.Close() }()
			if mapped := blob.BlobDigest(); mapped != recorded {
				t.Fatalf("mapped blob digest %x != footer digest %x", mapped[:8], recorded[:8])
			}
		})
	}
}

// A blob altered after it was written still parses and still carries its
// original footer. Reading the footer accepts it; hashing the bytes does not.
func TestVerifyBlobDigestRejectsAlteredBytesUnderAnIntactFooter(t *testing.T) {
	t.Parallel()
	path := buildDirectBlobFile(t)
	recorded, err := ReadBlobDigest(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // test temp path
	if err != nil {
		t.Fatal(err)
	}
	off := bytes.Index(data, []byte("digest test record"))
	if off < 0 {
		t.Fatal("premise: the record payload is stored verbatim in the blob")
	}
	patchFile(t, path, int64(off), []byte("D"))

	if _, err := OpenMappedBlob(path); err != nil {
		t.Fatalf("premise: the altered blob still parses: %v", err)
	}
	if after, err := ReadBlobDigest(path); err != nil || after != recorded {
		t.Fatalf("premise: the footer still records the original digest (err=%v)", err)
	}
	if _, err := VerifyBlobDigest(path); err == nil {
		t.Fatal("VerifyBlobDigest accepted a blob whose bytes no longer hash to its footer digest")
	}
}
