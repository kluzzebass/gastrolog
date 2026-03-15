package chunk

import "testing"

func FuzzDecodeWithDict(f *testing.F) {
	// Seed corpus.
	f.Add([]byte{0, 0})           // count=0 (valid empty)
	f.Add([]byte{})               // too short
	f.Add([]byte{0x01})           // truncated count
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}) // count=1, keyID=0, valID=0
	f.Add([]byte{0xff, 0xff})     // count=65535, way too many
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		dict := NewStringDict()
		// Populate dict so some ID lookups can succeed.
		dict.Add("host")       //nolint:errcheck
		dict.Add("srv-001")    //nolint:errcheck
		dict.Add("service")    //nolint:errcheck
		dict.Add("api")        //nolint:errcheck

		// Must never panic.
		_, _ = DecodeWithDict(data, dict)
	})
}

func FuzzDecodeDictData(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})                               // partial length field
	f.Add([]byte{3, 0, 'f', 'o', 'o'})                // one valid entry "foo"
	f.Add([]byte{3, 0, 'f', 'o', 'o', 3, 0, 'b'})    // second entry truncated
	f.Add([]byte{0xff, 0xff})                          // strLen=65535, no data
	f.Add(make([]byte, 200))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic.
		_, _ = DecodeDictData(data)
	})
}

func FuzzAttributesEncode(f *testing.F) {
	// Fuzz the Encode path with random key/value pairs.
	f.Add("key", "value")
	f.Add("", "")
	f.Add("a", "b")
	f.Add("host", "srv-001.example.com")

	f.Fuzz(func(t *testing.T, key, value string) {
		attrs := Attributes{key: value}
		encoded, err := attrs.Encode()
		if err != nil {
			return // ErrAttrsTooLarge is expected for huge inputs
		}
		// Verify count field.
		if len(encoded) < 2 {
			t.Fatal("encoded too short")
		}
	})
}
