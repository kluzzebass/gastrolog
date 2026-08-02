package glcb_test

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/tsindex"
)

// The registry is the single decode-dispatch point for TOC sections: readers
// hand it the TOC entry (which carries the section's type and version) plus
// the raw bytes, and get back the section kind's typed view. Format evolution
// means registering a codec for the new version — old sections keep decoding
// through the codec that matches their recorded version, no rewrite.

// buildBlobOnDisk writes a real blob with n records and returns its path.
func buildBlobOnDisk(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	w, err := glcb.NewWriter(chunk.NewChunkID(), glid.New(), dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	for i := range n {
		ts := base.Add(time.Duration(i) * time.Second)
		if err := w.Add(chunk.Record{
			SourceTS: ts, IngestTS: ts, WriteTS: ts,
			Raw: []byte("registry-test-record"),
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

// The production path: a real writer's ITSI section resolves through the
// default registry to a TS-index view that answers searches.
func TestDefaultRegistryServesWriterITSI(t *testing.T) {
	t.Parallel()
	path := buildBlobOnDisk(t, 10)

	entry, data, closer, err := glcb.MapSection(path, glcb.SectionIngestTSIndex)
	if err != nil {
		t.Fatalf("MapSection: %v", err)
	}
	defer func() { _ = closer() }()

	view, err := glcb.DefaultRegistry().NewView(entry, data)
	if err != nil {
		t.Fatalf("NewView: %v", err)
	}
	ts, ok := view.(tsindex.View)
	if !ok {
		t.Fatalf("view is %T, want tsindex.View", view)
	}
	if ts.Len() != 10 {
		t.Errorf("Len = %d, want 10", ts.Len())
	}
	probe := time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC).UnixNano()
	rank, _, ok := ts.SearchTS(probe)
	if !ok || rank != 5 {
		t.Errorf("SearchTS(+5s) = (%d, %v), want rank 5", rank, ok)
	}
}

// syntheticV2 models a format bump: same section kind, different byte layout
// (big-endian entries behind a u32 count header). The registry must route v1
// and v2 sections to their own codecs, and both must answer identically —
// that is what "old chunks readable after format evolution" means.
type syntheticV2 struct{ sectionType byte }

func (c syntheticV2) SectionType() byte     { return c.sectionType }
func (c syntheticV2) SectionVersion() uint8 { return 2 }
func (c syntheticV2) NewView(data []byte) (any, error) {
	if len(data) < 4 {
		return nil, errors.New("v2: missing count header")
	}
	n := binary.BigEndian.Uint32(data[:4])
	entries := make([]tsindex.Entry, n)
	for i := range entries {
		off := 4 + i*12
		entries[i] = tsindex.Entry{
			TS:  int64(binary.BigEndian.Uint64(data[off:])), //nolint:gosec // test codec
			Pos: binary.BigEndian.Uint32(data[off+8:]),
		}
	}
	return v2View{entries: entries}, nil
}

type v2View struct{ entries []tsindex.Entry }

func (v v2View) SearchTS(tsNano int64) (uint32, uint32, bool) {
	for i, e := range v.entries {
		if e.TS >= tsNano {
			return uint32(i), e.Pos, true //nolint:gosec // test codec
		}
	}
	return 0, 0, false
}
func (v v2View) Len() uint32                    { return uint32(len(v.entries)) } //nolint:gosec // test codec
func (v v2View) EntryAt(i uint32) tsindex.Entry { return v.entries[i] }

func encodeV2(entries []tsindex.Entry) []byte {
	buf := make([]byte, 4+len(entries)*12)
	binary.BigEndian.PutUint32(buf, uint32(len(entries))) //nolint:gosec // test codec
	for i, e := range entries {
		off := 4 + i*12
		binary.BigEndian.PutUint64(buf[off:], uint64(e.TS)) //nolint:gosec // test codec
		binary.BigEndian.PutUint32(buf[off+8:], e.Pos)
	}
	return buf
}

// The migration case 4jxqz's acceptance names: sections written under v1 keep
// decoding through the v1 codec after a v2 codec exists, and both versions
// answer the same question the same way.
func TestRegistryDispatchesAcrossVersions(t *testing.T) {
	t.Parallel()
	reg, err := glcb.NewRegistry(append(glcb.BuiltinSectionCodecs(),
		syntheticV2{sectionType: glcb.SectionIngestTSIndex})...)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	entries := []tsindex.Entry{{TS: 100, Pos: 1}, {TS: 200, Pos: 2}}

	v1Entry := glcb.TOCEntry{Type: glcb.SectionIngestTSIndex, Version: 1}
	v1View, err := reg.NewView(v1Entry, tsindex.EncodeAll(entries))
	if err != nil {
		t.Fatalf("v1 NewView: %v", err)
	}
	v2Entry := glcb.TOCEntry{Type: glcb.SectionIngestTSIndex, Version: 2}
	v2View, err := reg.NewView(v2Entry, encodeV2(entries))
	if err != nil {
		t.Fatalf("v2 NewView: %v", err)
	}

	for _, probe := range []int64{50, 150, 200} {
		r1, p1, ok1 := v1View.(tsindex.View).SearchTS(probe)
		r2, p2, ok2 := v2View.(tsindex.View).SearchTS(probe)
		if r1 != r2 || p1 != p2 || ok1 != ok2 {
			t.Errorf("probe %d: v1=(%d,%d,%v) v2=(%d,%d,%v) — versions disagree",
				probe, r1, p1, ok1, r2, p2, ok2)
		}
	}
}

// A version nothing registered must fail loudly and distinctly — the caller
// treats it like an unreadable section, not a crash and not silent zeros.
func TestRegistryUnknownVersionIsDistinct(t *testing.T) {
	t.Parallel()
	entry := glcb.TOCEntry{Type: glcb.SectionIngestTSIndex, Version: 9}
	_, err := glcb.DefaultRegistry().NewView(entry, nil)
	if !errors.Is(err, glcb.ErrNoSectionCodec) {
		t.Fatalf("err = %v, want ErrNoSectionCodec", err)
	}
}

func TestRegistryUnknownTypeIsDistinct(t *testing.T) {
	t.Parallel()
	entry := glcb.TOCEntry{Type: 0xEE, Version: 1}
	_, err := glcb.DefaultRegistry().NewView(entry, nil)
	if !errors.Is(err, glcb.ErrNoSectionCodec) {
		t.Fatalf("err = %v, want ErrNoSectionCodec", err)
	}
}

// Two codecs claiming one (type, version) is a wiring bug; last-writer-wins
// would make decode behavior depend on registration order.
func TestRegistryRejectsDuplicateCodec(t *testing.T) {
	t.Parallel()
	dup := syntheticV2{sectionType: glcb.SectionIngestTSIndex}
	if _, err := glcb.NewRegistry(dup, dup); err == nil {
		t.Fatal("duplicate (type, version) registration accepted")
	}
}
