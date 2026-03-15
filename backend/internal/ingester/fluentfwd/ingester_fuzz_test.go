package fluentfwd

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// FuzzDecodeTime fuzzes the decodeTime function which handles integer,
// float, and EventTime extension type values from msgpack input.
func FuzzDecodeTime(f *testing.F) {
	// Integer timestamp.
	intBuf, _ := msgpack.Marshal(int64(1700000000))
	f.Add(intBuf)

	// Float timestamp.
	floatBuf, _ := msgpack.Marshal(float64(1700000000.123))
	f.Add(floatBuf)

	// Uint timestamp.
	uintBuf, _ := msgpack.Marshal(uint64(1700000000))
	f.Add(uintBuf)

	// Nil / empty.
	f.Add([]byte{})
	f.Add([]byte{0xc0}) // msgpack nil

	// Random-ish bytes.
	f.Add([]byte{0xff, 0x00, 0x01, 0x02})
	f.Add([]byte{0x92, 0x01, 0x02}) // small array

	f.Fuzz(func(t *testing.T, data []byte) {
		dec := msgpack.NewDecoder(bytes.NewReader(data))
		ts, err := decodeTime(dec)
		_ = ts
		_ = err
	})
}

// FuzzDecodeRecord fuzzes the decodeRecord function which decodes
// a msgpack map into map[string]any.
func FuzzDecodeRecord(f *testing.F) {
	// Valid map.
	mapBuf, _ := msgpack.Marshal(map[string]any{"message": "hello", "level": "info"})
	f.Add(mapBuf)

	// Empty map.
	emptyBuf, _ := msgpack.Marshal(map[string]any{})
	f.Add(emptyBuf)

	// Not a map.
	f.Add([]byte{0xc0}) // nil
	f.Add([]byte{0x91, 0x01}) // array
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		dec := msgpack.NewDecoder(bytes.NewReader(data))
		rec, err := decodeRecord(dec)
		_ = rec
		_ = err
	})
}

// FuzzEventTimeUnmarshal fuzzes the EventTime extension unmarshal.
func FuzzEventTimeUnmarshal(f *testing.F) {
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0x65, 0x5e, 0x1a, 0x00, 0x07, 0x5b, 0xca, 0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0x3b, 0x9a, 0xc9, 0xff})
	f.Add([]byte{})
	f.Add([]byte{0x01, 0x02, 0x03})

	f.Fuzz(func(t *testing.T, data []byte) {
		et := &eventTime{}
		_ = et.UnmarshalMsgpack(data)
	})
}

// FuzzIsArrayCode fuzzes the isArrayCode helper.
func FuzzIsArrayCode(f *testing.F) {
	f.Add(byte(0x90))
	f.Add(byte(0x9f))
	f.Add(byte(0xdc))
	f.Add(byte(0xdd))
	f.Add(byte(0x00))
	f.Add(byte(0xff))

	f.Fuzz(func(t *testing.T, c byte) {
		_ = isArrayCode(c)
	})
}
