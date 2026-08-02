package glcb_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/tsindex"
)

// The transport format exists so a byte range of the cloud object means
// something: per-section zstd frames plus a raw directory at the tail. A cold
// chunk's TS index is then reachable in KB-scale range GETs instead of a
// full-blob fetch, while the bulk — and any future fat index section — stays
// compressed.

// buildBulkyBlobOnDisk writes a blob whose records carry incompressible
// payloads, so the object's size is dominated by the body frame the way a
// production chunk's is. A compressible fixture would make the tail probe
// look like most of the object and the bytes-fetched guard meaningless.
func buildBulkyBlobOnDisk(t *testing.T, records, payloadLen int) string {
	t.Helper()
	dir := t.TempDir()
	w, err := glcb.NewWriter(chunk.NewChunkID(), glid.New(), dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic fixture
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	for i := range records {
		payload := make([]byte, payloadLen)
		_, _ = rng.Read(payload)
		ts := base.Add(time.Duration(i) * time.Second)
		if err := w.Add(chunk.Record{
			SourceTS: ts, IngestTS: ts, WriteTS: ts,
			Raw: payload,
		}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	f, err := os.CreateTemp(dir, "blob-*.glcb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteTo(f); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// wrapToStore builds a blob, wraps it for transport, uploads it to an
// in-memory store, and returns the local blob path, the store, the key and
// the object size.
func wrapToStore(t *testing.T, records int) (string, *blobstore.Memory, string, int64) {
	t.Helper()
	path := buildBlobOnDisk(t, records)
	var buf bytes.Buffer
	if _, err := glcb.WrapForTransport(&buf, path); err != nil {
		t.Fatalf("WrapForTransport: %v", err)
	}
	store := blobstore.NewMemory()
	const key = "vault-test/transport-test.glcb"
	if err := store.Upload(context.Background(), key, bytes.NewReader(buf.Bytes()), nil); err != nil {
		t.Fatalf("upload: %v", err)
	}
	return path, store, key, int64(buf.Len())
}

// Full download must reassemble the local GLCB byte-identically — the
// transport is a wrapper, not a format change on the blob itself.
func TestTransportRoundTripIsByteIdentical(t *testing.T) {
	t.Parallel()
	path, store, key, _ := wrapToStore(t, 50)

	dst, err := os.CreateTemp(t.TempDir(), "unwrap-*.glcb")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := glcb.DownloadAndUnwrap(context.Background(), store, key, dst); err != nil {
		t.Fatalf("DownloadAndUnwrap: %v", err)
	}

	got, err := os.ReadFile(dst.Name())
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("reassembled blob differs: got %d bytes, want %d (first divergence at %d)",
			len(got), len(want), firstDiff(got, want))
	}
}

func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// countingFetcher counts what a ranged read actually pulls, so the "bytes
// fetched ≪ object size" acceptance is measured rather than assumed.
type countingFetcher struct {
	store *blobstore.Memory
	key   string
	bytes int64
	gets  int
}

func (f *countingFetcher) DownloadRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	f.bytes += length
	f.gets++
	return f.store.DownloadRange(ctx, key, off, length)
}

// The point of the whole exercise: a cold chunk's ITSI resolves through
// range GETs, decodes through the section registry, and answers exactly
// like the local mmap view.
func TestFetchRemoteSectionMatchesLocalView(t *testing.T) {
	t.Parallel()
	path := buildBulkyBlobOnDisk(t, 200, 512)
	var buf bytes.Buffer
	if _, err := glcb.WrapForTransport(&buf, path); err != nil {
		t.Fatalf("WrapForTransport: %v", err)
	}
	store := blobstore.NewMemory()
	const key = "vault-test/bulky.glcb"
	if err := store.Upload(context.Background(), key, bytes.NewReader(buf.Bytes()), nil); err != nil {
		t.Fatalf("upload: %v", err)
	}
	objSize := int64(buf.Len())

	fetcher := &countingFetcher{store: store, key: key}
	entry, data, err := glcb.FetchRemoteSection(context.Background(), fetcher, key, objSize, glcb.SectionIngestTSIndex)
	if err != nil {
		t.Fatalf("FetchRemoteSection: %v", err)
	}
	remoteAny, err := glcb.DefaultRegistry().NewView(entry, data)
	if err != nil {
		t.Fatalf("registry view: %v", err)
	}
	remote := remoteAny.(tsindex.View)

	localEntry, localData, closer, err := glcb.MapSection(path, glcb.SectionIngestTSIndex)
	if err != nil {
		t.Fatalf("MapSection: %v", err)
	}
	defer func() { _ = closer() }()
	localAny, err := glcb.DefaultRegistry().NewView(localEntry, localData)
	if err != nil {
		t.Fatalf("local registry view: %v", err)
	}
	local := localAny.(tsindex.View)

	if remote.Len() != local.Len() {
		t.Fatalf("remote Len %d != local Len %d", remote.Len(), local.Len())
	}
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	for _, offset := range []time.Duration{0, 37 * time.Second, 199 * time.Second, 500 * time.Second} {
		probe := base.Add(offset).UnixNano()
		rr, rp, rok := remote.SearchTS(probe)
		lr, lp, lok := local.SearchTS(probe)
		if rr != lr || rp != lp || rok != lok {
			t.Errorf("probe +%v: remote=(%d,%d,%v) local=(%d,%d,%v)", offset, rr, rp, rok, lr, lp, lok)
		}
	}

	// The acceptance's measured half: a rank lookup must not approach a
	// full-blob fetch. Half is a generous ceiling — typical is a few percent —
	// but it fails loudly if someone reroutes this through a whole-object GET.
	if fetcher.bytes >= objSize/2 {
		t.Errorf("ranged read fetched %d of %d object bytes; that is not a ranged read", fetcher.bytes, objSize)
	}
	if fetcher.gets > 3 {
		t.Errorf("ranged read took %d GETs, want ≤ 3 (tail slice + section, +1 slack)", fetcher.gets)
	}
}

// A section the object does not carry must answer like a missing section,
// so callers keep one not-found path for local and remote reads.
func TestFetchRemoteSectionMissing(t *testing.T) {
	t.Parallel()
	_, store, key, objSize := wrapToStore(t, 10)

	fetcher := &countingFetcher{store: store, key: key}
	_, _, err := glcb.FetchRemoteSection(context.Background(), fetcher, key, objSize, glcb.SectionTokenIndex)
	if !errors.Is(err, glcb.ErrSectionNotFound) {
		t.Fatalf("err = %v, want ErrSectionNotFound", err)
	}
}

// A frame whose bytes do not hash to what the directory recorded is
// corruption; returning the bytes anyway would feed a search index garbage.
func TestFetchRemoteSectionDetectsCorruption(t *testing.T) {
	t.Parallel()
	path := buildBlobOnDisk(t, 30)
	var buf bytes.Buffer
	if _, err := glcb.WrapForTransport(&buf, path); err != nil {
		t.Fatalf("WrapForTransport: %v", err)
	}
	obj := buf.Bytes()
	// Flip one byte in the middle third of the object — inside some frame,
	// never inside the raw directory/footer tail.
	obj[len(obj)/2] ^= 0xFF

	store := blobstore.NewMemory()
	const key = "vault-test/corrupt.glcb"
	if err := store.Upload(context.Background(), key, bytes.NewReader(obj), nil); err != nil {
		t.Fatal(err)
	}
	fetcher := &countingFetcher{store: store, key: key}
	if _, _, err := glcb.FetchRemoteSection(context.Background(), fetcher, key, int64(len(obj)), glcb.SectionIngestTSIndex); err == nil {
		// The flipped byte may land in the body frame, which this fetch never
		// reads — only fail when the fetch READ corrupted bytes and said ok.
		dst, terr := os.CreateTemp(t.TempDir(), "unwrap-*.glcb")
		if terr != nil {
			t.Fatal(terr)
		}
		defer func() { _ = dst.Close() }()
		if uerr := glcb.DownloadAndUnwrap(context.Background(), store, key, dst); uerr == nil {
			t.Fatal("neither the ranged fetch nor the full unwrap noticed a corrupted frame")
		}
	}
}

// Truncation must be an error, not a short read presented as success.
func TestUnwrapTransportRejectsTruncatedObject(t *testing.T) {
	t.Parallel()
	path := buildBlobOnDisk(t, 30)
	var buf bytes.Buffer
	if _, err := glcb.WrapForTransport(&buf, path); err != nil {
		t.Fatal(err)
	}
	store := blobstore.NewMemory()
	const key = "vault-test/truncated.glcb"
	if err := store.Upload(context.Background(), key,
		bytes.NewReader(buf.Bytes()[:buf.Len()/2]), nil); err != nil {
		t.Fatal(err)
	}
	dst, err := os.CreateTemp(t.TempDir(), "unwrap-*.glcb")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := glcb.DownloadAndUnwrap(context.Background(), store, key, dst); err == nil {
		t.Fatal("truncated object unwrapped without error")
	}
}

// The old whole-blob zstd objects are gone per the pre-1.0 format policy;
// feeding one to the new unwrap must produce a loud error, not garbage.
func TestUnwrapTransportRejectsForeignBytes(t *testing.T) {
	t.Parallel()
	store := blobstore.NewMemory()
	const key = "vault-test/foreign.bin"
	if err := store.Upload(context.Background(), key,
		strings.NewReader("not a transport-framed object at all, nowhere near one"), nil); err != nil {
		t.Fatal(err)
	}
	dst, err := os.CreateTemp(t.TempDir(), "unwrap-*.glcb")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := glcb.DownloadAndUnwrap(context.Background(), store, key, dst); err == nil {
		t.Fatal("foreign bytes unwrapped without error")
	}
}

// A wrapped empty-chunk blob (zero records is legal) must round-trip too.
func TestTransportRoundTripEmptyChunk(t *testing.T) {
	t.Parallel()
	path, store, key, _ := wrapToStore(t, 0)

	dst, err := os.CreateTemp(t.TempDir(), "unwrap-*.glcb")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := glcb.DownloadAndUnwrap(context.Background(), store, key, dst); err != nil {
		t.Fatalf("DownloadAndUnwrap: %v", err)
	}
	got, _ := os.ReadFile(dst.Name())
	want, _ := os.ReadFile(path)
	if !bytes.Equal(got, want) {
		t.Fatal("empty-chunk blob does not round-trip")
	}
}

// Unused import guards for the chunk/glid types used by buildBlobOnDisk.
var (
	_ = chunk.Record{}
	_ = glid.New
)
