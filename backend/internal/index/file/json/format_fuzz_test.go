package json

import (
	"encoding/binary"
	"testing"

	"gastrolog/internal/format"
)

func FuzzDecodeJSONIndex(f *testing.F) {
	// Valid empty JSON index: header(4) + status(1) + offsets(28) = 33 bytes
	valid := make([]byte, fileHeaderSize)
	h := format.Header{Type: format.TypeJSONIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(valid)
	valid[format.HeaderSize] = statusComplete

	// Offsets: dict at fileHeaderSize, 0 entries for everything, blob at fileHeaderSize
	off := format.HeaderSize + statusSize
	binary.LittleEndian.PutUint32(valid[off:], uint32(fileHeaderSize))    // dictOffset
	binary.LittleEndian.PutUint32(valid[off+4:], 0)                      // dictCount
	binary.LittleEndian.PutUint32(valid[off+8:], uint32(fileHeaderSize))  // pathOffset
	binary.LittleEndian.PutUint32(valid[off+12:], 0)                     // pathCount
	binary.LittleEndian.PutUint32(valid[off+16:], uint32(fileHeaderSize)) // pvOffset
	binary.LittleEndian.PutUint32(valid[off+20:], 0)                     // pvCount
	binary.LittleEndian.PutUint32(valid[off+24:], uint32(fileHeaderSize)) // blobOffset
	f.Add(valid)

	// Valid index with one dict entry ("msg"), one path entry, zero PV entries
	// dict: len(2) + "msg"(3) = 5 bytes
	// path table: 1 entry * 12 = 12 bytes
	// blob: empty
	totalSize := fileHeaderSize + 5 + 12
	buf := make([]byte, totalSize)
	h.EncodeInto(buf)
	buf[format.HeaderSize] = statusComplete
	dictOff := uint32(fileHeaderSize)
	pathOff := dictOff + 5
	pvOff := pathOff + 12
	blobOff := pvOff
	o := format.HeaderSize + statusSize
	binary.LittleEndian.PutUint32(buf[o:], dictOff)
	binary.LittleEndian.PutUint32(buf[o+4:], 1)  // dictCount=1
	binary.LittleEndian.PutUint32(buf[o+8:], pathOff)
	binary.LittleEndian.PutUint32(buf[o+12:], 1) // pathCount=1
	binary.LittleEndian.PutUint32(buf[o+16:], pvOff)
	binary.LittleEndian.PutUint32(buf[o+20:], 0) // pvCount=0
	binary.LittleEndian.PutUint32(buf[o+24:], blobOff)
	// dict entry: "msg"
	dc := int(dictOff)
	binary.LittleEndian.PutUint16(buf[dc:], 3)
	copy(buf[dc+2:], "msg")
	// path entry: dictID=0, blobOffset=0, count=0
	pc := int(pathOff)
	binary.LittleEndian.PutUint32(buf[pc:], 0)
	binary.LittleEndian.PutUint32(buf[pc+4:], 0)
	binary.LittleEndian.PutUint32(buf[pc+8:], 0)
	f.Add(buf)

	// Edge cases
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	// Capped status
	capped := make([]byte, len(valid))
	copy(capped, valid)
	capped[format.HeaderSize] = statusCapped
	f.Add(capped)

	// Invalid status byte
	badStatus := make([]byte, len(valid))
	copy(badStatus, valid)
	badStatus[format.HeaderSize] = 0xAB
	f.Add(badStatus)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic on any input.
		_, _, _, _ = decodeIndex(data)
	})
}
