package btree

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"

	"gastrolog/internal/format"
)

// compare is the function signature for key comparison.
type compare[K any] func(K, K) int

const (
	pageSize = 4096

	typeLeaf     byte = 0x01
	typeInternal byte = 0x02

	// leafHeaderSize: type (1) + count (2) + nextLeaf (4) + prevLeaf (4).
	leafHeaderSize = 11
	// internalHeaderSize: type (1) + count (2).
	internalHeaderSize = 3

	metaVersion = 1
)

// Codec defines how keys and values are serialized to and from bytes.
type Codec[K, V any] struct {
	KeySize int            // encoded size of a key in bytes
	ValSize int            // encoded size of a value in bytes
	PutKey  func([]byte, K)
	Key     func([]byte) K
	PutVal  func([]byte, V)
	Val     func([]byte) V
	Compare compare[K]     // key comparison function
}

// Int64Uint32 is a codec for int64 keys and uint32 values,
// suitable for mapping nanosecond timestamps to record positions.
var Int64Uint32 = Codec[int64, uint32]{
	KeySize: 8,
	ValSize: 4,
	PutKey:  func(b []byte, k int64) { binary.LittleEndian.PutUint64(b, uint64(k)) },
	Key:     func(b []byte) int64 { return int64(binary.LittleEndian.Uint64(b)) },
	PutVal:  func(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) },
	Val:     func(b []byte) uint32 { return binary.LittleEndian.Uint32(b) },
	Compare: cmp.Compare[int64],
}

// Entry is a key-value pair stored in the tree.
type Entry[K, V any] struct {
	Key   K
	Value V
}

type node[K, V any] struct {
	typ byte

	// Leaf fields.
	entries  []Entry[K, V]
	nextLeaf uint32
	prevLeaf uint32

	// Internal fields.
	keys     []K
	children []uint32
}

type meta struct {
	root     uint32
	count    uint64
	height   uint16
	nextPage uint32
	keySize  uint16 // stored for codec validation on Open
	valSize  uint16
}

// --- encoding ----------------------------------------------------------------

func encodeMeta(m *meta, buf []byte) {
	clear(buf[:pageSize])
	format.Header{Type: format.TypeBTree, Version: metaVersion}.EncodeInto(buf)
	binary.LittleEndian.PutUint32(buf[4:8], m.root)
	binary.LittleEndian.PutUint64(buf[8:16], m.count)
	binary.LittleEndian.PutUint16(buf[16:18], m.height)
	binary.LittleEndian.PutUint32(buf[18:22], m.nextPage)
	binary.LittleEndian.PutUint16(buf[22:24], m.keySize)
	binary.LittleEndian.PutUint16(buf[24:26], m.valSize)
}

func decodeMeta(buf []byte, m *meta) error {
	if len(buf) < 26 {
		return errors.New("btree: meta page too small")
	}
	if _, err := format.DecodeAndValidate(buf[:format.HeaderSize], format.TypeBTree, metaVersion); err != nil {
		return fmt.Errorf("btree: %w", err)
	}
	m.root = binary.LittleEndian.Uint32(buf[4:8])
	m.count = binary.LittleEndian.Uint64(buf[8:16])
	m.height = binary.LittleEndian.Uint16(buf[16:18])
	m.nextPage = binary.LittleEndian.Uint32(buf[18:22])
	m.keySize = binary.LittleEndian.Uint16(buf[22:24])
	m.valSize = binary.LittleEndian.Uint16(buf[24:26])
	return nil
}

func encodeNode[K, V any](n *node[K, V], c *Codec[K, V], buf []byte) {
	clear(buf[:pageSize])
	switch n.typ {
	case typeLeaf:
		buf[0] = typeLeaf
		binary.LittleEndian.PutUint16(buf[1:3], uint16(len(n.entries))) //nolint:gosec // bounded by maxLeaf
		binary.LittleEndian.PutUint32(buf[3:7], n.nextLeaf)
		binary.LittleEndian.PutUint32(buf[7:11], n.prevLeaf)
		off := leafHeaderSize
		for _, e := range n.entries {
			c.PutKey(buf[off:], e.Key)
			off += c.KeySize
			c.PutVal(buf[off:], e.Value)
			off += c.ValSize
		}
	case typeInternal:
		buf[0] = typeInternal
		binary.LittleEndian.PutUint16(buf[1:3], uint16(len(n.keys))) //nolint:gosec // bounded by maxInternal
		off := internalHeaderSize
		for _, k := range n.keys {
			c.PutKey(buf[off:], k)
			off += c.KeySize
		}
		for _, ch := range n.children {
			binary.LittleEndian.PutUint32(buf[off:off+4], ch)
			off += 4
		}
	}
}

func decodeNode[K, V any](buf []byte, c *Codec[K, V]) (*node[K, V], error) {
	if len(buf) < internalHeaderSize {
		return nil, errors.New("btree: page too small")
	}
	switch buf[0] {
	case typeLeaf:
		count := binary.LittleEndian.Uint16(buf[1:3])
		n := &node[K, V]{
			typ:      typeLeaf,
			nextLeaf: binary.LittleEndian.Uint32(buf[3:7]),
			prevLeaf: binary.LittleEndian.Uint32(buf[7:11]),
			entries:  make([]Entry[K, V], count),
		}
		off := leafHeaderSize
		for i := range count {
			n.entries[i] = Entry[K, V]{
				Key:   c.Key(buf[off:]),
				Value: c.Val(buf[off+c.KeySize:]),
			}
			off += c.KeySize + c.ValSize
		}
		return n, nil
	case typeInternal:
		numKeys := binary.LittleEndian.Uint16(buf[1:3])
		n := &node[K, V]{
			typ:      typeInternal,
			keys:     make([]K, numKeys),
			children: make([]uint32, numKeys+1),
		}
		off := internalHeaderSize
		for i := range numKeys {
			n.keys[i] = c.Key(buf[off:])
			off += c.KeySize
		}
		for i := range numKeys + 1 {
			n.children[i] = binary.LittleEndian.Uint32(buf[off : off+4])
			off += 4
		}
		return n, nil
	default:
		return nil, fmt.Errorf("btree: unknown node type 0x%02x", buf[0])
	}
}
