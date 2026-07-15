package chunk

import (
	"sync"
	"testing"
	"unsafe"
)

func TestMmapStringDictRoundTrip(t *testing.T) {
	t.Parallel()
	dict := NewStringDict()
	words := []string{"host", "web-1", "level", "info", "error"}
	for _, w := range words {
		if _, err := dict.Add(w); err != nil {
			t.Fatal(err)
		}
	}
	var buf []byte
	for _, s := range dict.strings {
		buf = append(buf, EncodeDictEntry(s)...)
	}

	mmapDict, err := NewMmapStringDict(buf, uint32(len(words))) //nolint:gosec // G115: test slice len
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range words {
		got, err := mmapDict.Get(uint32(i)) //nolint:gosec // G115: test index
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if got != want {
			t.Fatalf("Get(%d) = %q, want %q", i, got, want)
		}
	}
}

// TestMmapStringDictInternsAcrossGets pins the interning contract: repeated
// Gets for the same ID return the identical heap string (no per-lookup
// copy), and interned strings stay valid after the backing region's bytes
// are gone — the retain-after-mmap-release guarantee callers rely on.
func TestMmapStringDictInternsAcrossGets(t *testing.T) {
	t.Parallel()
	buf := append(append([]byte{}, EncodeDictEntry("host")...), EncodeDictEntry("web-1")...)
	dict, err := NewMmapStringDict(buf, 2)
	if err != nil {
		t.Fatal(err)
	}
	first, err := dict.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := dict.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.StringData(first) != unsafe.StringData(second) {
		t.Fatal("repeated Get returned distinct copies; expected the interned string")
	}
	// Simulate mmap release: clobber the backing bytes. Interned strings
	// must be unaffected.
	for i := range buf {
		buf[i] = 0
	}
	got, err := dict.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if got != "web-1" {
		t.Fatalf("interned string corrupted after backing release: %q", got)
	}
}

// TestMmapStringDictConcurrentGets exercises first-access interning races
// under -race: concurrent Gets for the same IDs must all observe correct
// values.
func TestMmapStringDictConcurrentGets(t *testing.T) {
	t.Parallel()
	words := []string{"host", "web-1", "level", "info", "error"}
	var buf []byte
	for _, w := range words {
		buf = append(buf, EncodeDictEntry(w)...)
	}
	dict, err := NewMmapStringDict(buf, uint32(len(words))) //nolint:gosec // G115: test slice len
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i, want := range words {
				got, err := dict.Get(uint32(i)) //nolint:gosec // G115: test index
				if err != nil || got != want {
					t.Errorf("Get(%d) = %q, %v; want %q", i, got, err, want)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestMmapStringDictScanToleratesTrailingPartial(t *testing.T) {
	t.Parallel()
	entry := EncodeDictEntry("partial")
	buf := append(append([]byte{}, entry...), 0x01) // truncated next entry

	mmapDict, err := ScanMmapStringDict(buf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := mmapDict.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if got != "partial" {
		t.Fatalf("got %q", got)
	}
}
