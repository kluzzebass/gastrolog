package glid

import "testing"

func FuzzParseRoundTrip(f *testing.F) {
	f.Add("00000000000000000000000000")
	f.Add(New().String())

	f.Fuzz(func(t *testing.T, s string) {
		id, err := Parse(s)
		if err != nil {
			return // invalid input, skip
		}
		// Valid parse → round-trip must succeed.
		s2 := id.String()
		id2, err := Parse(s2)
		if err != nil {
			t.Fatalf("round-trip failed: Parse(%q) → %s → Parse(%q): %v", s, id, s2, err)
		}
		if id != id2 {
			t.Fatalf("round-trip mismatch: %s != %s", id, id2)
		}
	})
}

func FuzzBytesRoundTrip(f *testing.F) {
	f.Add(New().Bytes())
	f.Add(Nil.Bytes())
	f.Add(make([]byte, 0))
	f.Add(make([]byte, 15))

	f.Fuzz(func(t *testing.T, b []byte) {
		id := FromBytes(b)
		if len(b) < Size {
			if !id.IsZero() {
				t.Fatal("short input should produce Nil")
			}
			return
		}
		b2 := id.Bytes()
		id2 := FromBytes(b2)
		if id != id2 {
			t.Fatal("bytes round-trip mismatch")
		}
	})
}
