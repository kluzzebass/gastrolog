package command

import (
	"testing"
)

// FuzzCommandUnmarshal feeds random bytes to the protobuf command
// deserializer. It must return an error for invalid input, never panic.
func FuzzCommandUnmarshal(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte("not a protobuf"))
	f.Add([]byte{0x0a, 0x02, 0x08, 0x01}) // small valid-looking varint

	f.Fuzz(func(t *testing.T, data []byte) {
		// Unmarshal may succeed (protobuf is lenient) or fail; must not panic.
		_, _ = Unmarshal(data)
	})
}

// FuzzSnapshotUnmarshal feeds random bytes to the snapshot deserializer.
func FuzzSnapshotUnmarshal(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte("not a protobuf"))

	f.Fuzz(func(t *testing.T, data []byte) {
		snap, err := UnmarshalSnapshot(data)
		if err != nil {
			return
		}
		// If unmarshal succeeds, RestoreSnapshot must not panic either.
		_, _, _, _, _ = RestoreSnapshot(snap)
	})
}

// FuzzExtractPutServerSettings feeds random JSON strings to the server
// settings parser. It must return an error or a valid struct, never panic.
func FuzzExtractPutServerSettings(f *testing.F) {
	f.Add("")
	f.Add("{}")
	f.Add(`{"auth":{}}`)
	f.Add(`{"auth":{"min_password_length":8,"require_mixed_case":true}}`)
	f.Add("not json")
	f.Add(`{"auth":{"password_policy":{"min_length":12}}}`)

	f.Fuzz(func(t *testing.T, data string) {
		_, _ = ExtractPutServerSettings(data)
	})
}
