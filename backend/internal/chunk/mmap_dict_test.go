package chunk

import (
	"testing"
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
