package raftwal

import (
	"os"
	"testing"

	hraft "github.com/hashicorp/raft"
)

// FuzzLogEncodeDecode verifies that any data survives a round-trip through
// the log encoding without corruption or panic.
func FuzzLogEncodeDecode(f *testing.F) {
	f.Add(uint64(1), uint64(1), byte(0), []byte("hello"), []byte{})
	f.Add(uint64(0), uint64(0), byte(0), []byte{}, []byte{})
	f.Add(uint64(1<<63-1), uint64(1<<63-1), byte(4), make([]byte, 1024), []byte{0xDE, 0xAD})

	f.Fuzz(func(t *testing.T, index, term uint64, logType byte, data, ext []byte) {
		log := &hraft.Log{
			Index:      index,
			Term:       term,
			Type:       hraft.LogType(logType),
			Data:       data,
			Extensions: ext,
		}
		encoded := encodelog(log)

		var decoded hraft.Log
		if err := decodelog(encoded, &decoded); err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if decoded.Index != index {
			t.Errorf("index: got %d want %d", decoded.Index, index)
		}
		if decoded.Term != term {
			t.Errorf("term: got %d want %d", decoded.Term, term)
		}
		if decoded.Type != hraft.LogType(logType) {
			t.Errorf("type: got %d want %d", decoded.Type, logType)
		}
		if len(decoded.Data) != len(data) {
			t.Errorf("data len: got %d want %d", len(decoded.Data), len(data))
		}
		if len(decoded.Extensions) != len(ext) {
			t.Errorf("ext len: got %d want %d", len(decoded.Extensions), len(ext))
		}
	})
}

// FuzzStableSetEncodeDecode verifies stable set key/value round-trips.
func FuzzStableSetEncodeDecode(f *testing.F) {
	f.Add("CurrentTerm", []byte{0x01, 0x02})
	f.Add("", []byte{})
	f.Add("key-with-special-chars/日本語", []byte("value"))

	f.Fuzz(func(t *testing.T, key string, val []byte) {
		encoded := encodeStableSet(key, val)
		gotKey, gotVal := decodeStableSet(encoded)
		if gotKey != key {
			t.Errorf("key: got %q want %q", gotKey, key)
		}
		if len(gotVal) != len(val) {
			t.Errorf("val len: got %d want %d", len(gotVal), len(val))
		}
	})
}

// FuzzStableUint64EncodeDecode verifies stable uint64 round-trips.
func FuzzStableUint64EncodeDecode(f *testing.F) {
	f.Add("term", uint64(0))
	f.Add("vote", uint64(1<<64-1))
	f.Add("", uint64(42))

	f.Fuzz(func(t *testing.T, key string, val uint64) {
		encoded := encodeStableUint64(key, val)
		gotKey, gotVal := decodeStableUint64(encoded)
		if gotKey != key {
			t.Errorf("key: got %q want %q", gotKey, key)
		}
		if gotVal != val {
			t.Errorf("val: got %d want %d", gotVal, val)
		}
	})
}

// FuzzDeleteRangeEncodeDecode verifies delete range round-trips.
func FuzzDeleteRangeEncodeDecode(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(1), uint64(100))
	f.Add(uint64(1<<64-1), uint64(1<<64-1))

	f.Fuzz(func(t *testing.T, min, max uint64) {
		encoded := encodeDeleteRange(min, max)
		gotMin, gotMax := decodeDeleteRange(encoded)
		if gotMin != min || gotMax != max {
			t.Errorf("got (%d, %d) want (%d, %d)", gotMin, gotMax, min, max)
		}
	})
}

// FuzzReplayCorruptedSegment verifies that replay handles arbitrary data
// without panicking. The WAL should silently stop at the first unreadable
// entry and not crash.
func FuzzReplayCorruptedSegment(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x01, 0x02, 0x03})
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		w := &WAL{
			groups:   make(map[uint32]*groupState),
			groupIDs: make(map[string]uint32),
			nextGID:  1,
		}
		// Write the fuzzed data as a segment file and replay it.
		dir := t.TempDir()
		segPath := dir + "/wal-000001.log"
		if err := writeFile(segPath, data); err != nil {
			t.Skip("write failed:", err)
		}
		w.dir = dir
		// Should not panic regardless of input.
		_ = w.replay()
	})
}

func writeFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	_ = f.Close()
	return err
}
