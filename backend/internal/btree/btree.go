// Package btree implements a file-backed B+ tree.
//
// The tree maps ordered keys to ordered values. Duplicate keys are allowed;
// entries with equal keys are secondarily ordered by value.
//
// Uses pread/pwrite for I/O with an in-process page cache.
// Not safe for concurrent use; callers must synchronize access.
package btree

import (
	"cmp"
	"fmt"
	"os"
	"slices"
)

// Tree is a file-backed B+ tree.
type Tree[K cmp.Ordered, V cmp.Ordered] struct {
	file        *os.File
	codec       Codec[K, V]
	meta        meta
	pages       map[uint32]*node[K, V]
	dirty       map[uint32]bool
	metaDirty   bool
	maxLeaf     int // max entries per leaf
	maxInternal int // max keys per internal node
}

// Create creates a new, empty B+ tree at path using the given codec.
func Create[K cmp.Ordered, V cmp.Ordered](path string, codec Codec[K, V]) (*Tree[K, V], error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // G304: path is caller-controlled
	if err != nil {
		return nil, fmt.Errorf("btree create: %w", err)
	}
	t := &Tree[K, V]{
		file:  f,
		codec: codec,
		pages: make(map[uint32]*node[K, V]),
		dirty: make(map[uint32]bool),
		meta: meta{
			root:     1,
			height:   1,
			nextPage: 2,
			keySize:  uint16(codec.KeySize), //nolint:gosec // codec sizes are small
			valSize:  uint16(codec.ValSize), //nolint:gosec // codec sizes are small
		},
		metaDirty: true,
	}
	t.computeCapacity()

	// Empty root leaf.
	t.pages[1] = &node[K, V]{typ: typeLeaf}
	t.dirty[1] = true

	if err := t.flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return t, nil
}

// Open opens an existing B+ tree at path.
// The codec must match the one used to create the tree.
func Open[K cmp.Ordered, V cmp.Ordered](path string, codec Codec[K, V]) (*Tree[K, V], error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0) //nolint:gosec // G304: path is caller-controlled
	if err != nil {
		return nil, fmt.Errorf("btree open: %w", err)
	}
	t := &Tree[K, V]{
		file:  f,
		codec: codec,
		pages: make(map[uint32]*node[K, V]),
		dirty: make(map[uint32]bool),
	}
	var buf [pageSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("btree read meta: %w", err)
	}
	if err := decodeMeta(buf[:], &t.meta); err != nil {
		_ = f.Close()
		return nil, err
	}
	if int(t.meta.keySize) != codec.KeySize || int(t.meta.valSize) != codec.ValSize {
		_ = f.Close()
		return nil, fmt.Errorf("btree: codec mismatch: file has key=%d val=%d, codec has key=%d val=%d",
			t.meta.keySize, t.meta.valSize, codec.KeySize, codec.ValSize)
	}
	t.computeCapacity()
	return t, nil
}

// Count returns the total number of entries.
func (t *Tree[K, V]) Count() uint64 { return t.meta.count }

// Height returns the tree height (1 = root is a leaf).
func (t *Tree[K, V]) Height() uint16 { return t.meta.height }

// DiskSize returns the current on-disk size in bytes (pages × pageSize).
func (t *Tree[K, V]) DiskSize() uint64 { return uint64(t.meta.nextPage) * pageSize }

// Insert adds a key-value pair to the tree.
func (t *Tree[K, V]) Insert(key K, value V) error {
	e := Entry[K, V]{Key: key, Value: value}

	path := make([]pathEntry, 0, t.meta.height)
	pageNum := t.meta.root

	for {
		n, err := t.getNode(pageNum)
		if err != nil {
			return err
		}
		if n.typ == typeLeaf {
			pos, _ := slices.BinarySearchFunc(n.entries, e, compareEntries[K, V])
			n.entries = slices.Insert(n.entries, pos, e)
			t.markDirty(pageNum)
			t.meta.count++
			t.metaDirty = true

			if len(n.entries) > t.maxLeaf {
				return t.splitLeaf(path, pageNum, n)
			}
			return nil
		}

		childIdx := findChild(n.keys, key)
		path = append(path, pathEntry{page: pageNum, idx: childIdx})
		pageNum = n.children[childIdx]
	}
}

// FindGE returns an iterator at the first entry with Key >= key.
// The returned iterator is invalid if no such entry exists.
func (t *Tree[K, V]) FindGE(key K) (*Iter[K, V], error) {
	pageNum := t.meta.root
	for {
		n, err := t.getNode(pageNum)
		if err != nil {
			return nil, err
		}
		if n.typ == typeLeaf {
			return t.findInLeaf(n, pageNum, key)
		}
		pageNum = n.children[findChild(n.keys, key)]
	}
}

func (t *Tree[K, V]) findInLeaf(leaf *node[K, V], leafNum uint32, key K) (*Iter[K, V], error) {
	idx, _ := slices.BinarySearchFunc(leaf.entries, key, func(e Entry[K, V], target K) int {
		return cmp.Compare(e.Key, target)
	})
	if idx >= len(leaf.entries) {
		if leaf.nextLeaf == 0 {
			return &Iter[K, V]{}, nil
		}
		next, err := t.getNode(leaf.nextLeaf)
		if err != nil {
			return nil, err
		}
		return &Iter[K, V]{tree: t, leaf: next, leafNum: leaf.nextLeaf}, nil
	}
	return &Iter[K, V]{tree: t, leaf: leaf, leafNum: leafNum, pos: idx}, nil
}

// Scan returns an iterator at the first (smallest) entry.
// The returned iterator is invalid if the tree is empty.
func (t *Tree[K, V]) Scan() (*Iter[K, V], error) {
	pageNum := t.meta.root
	for {
		n, err := t.getNode(pageNum)
		if err != nil {
			return nil, err
		}
		if n.typ == typeLeaf {
			if len(n.entries) == 0 {
				return &Iter[K, V]{}, nil
			}
			return &Iter[K, V]{tree: t, leaf: n, leafNum: pageNum}, nil
		}
		pageNum = n.children[0]
	}
}

// Sync writes all dirty pages and fsyncs the file.
func (t *Tree[K, V]) Sync() error {
	if err := t.flush(); err != nil {
		return err
	}
	return t.file.Sync()
}

// Close writes dirty pages and closes the file.
// Does not fsync — call Sync first if durability is needed.
func (t *Tree[K, V]) Close() error {
	err := t.flush()
	cerr := t.file.Close()
	if err != nil {
		return err
	}
	return cerr
}

// --- internal ----------------------------------------------------------------

type pathEntry struct {
	page uint32
	idx  int // index into parent.children
}

func (t *Tree[K, V]) computeCapacity() {
	entrySize := t.codec.KeySize + t.codec.ValSize
	t.maxLeaf = (pageSize - leafHeaderSize) / entrySize
	t.maxInternal = (pageSize - internalHeaderSize - 4) / (t.codec.KeySize + 4)
}

// findChild returns the child index to descend into for key.
// Standard B+ tree descent: first child i where keys[i] > key.
func findChild[K cmp.Ordered](keys []K, key K) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if keys[mid] <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (t *Tree[K, V]) splitLeaf(path []pathEntry, leafNum uint32, leaf *node[K, V]) error {
	mid := len(leaf.entries) / 2

	rightNum := t.allocPage()
	right := &node[K, V]{
		typ:      typeLeaf,
		entries:  slices.Clone(leaf.entries[mid:]),
		nextLeaf: leaf.nextLeaf,
		prevLeaf: leafNum,
	}

	// Update the successor leaf's back-pointer.
	if leaf.nextLeaf != 0 {
		succ, err := t.getNode(leaf.nextLeaf)
		if err != nil {
			return err
		}
		succ.prevLeaf = rightNum
		t.markDirty(leaf.nextLeaf)
	}

	leaf.entries = slices.Clone(leaf.entries[:mid])
	leaf.nextLeaf = rightNum

	t.pages[rightNum] = right
	t.markDirty(rightNum)
	t.markDirty(leafNum)

	return t.insertIntoParent(path, leafNum, right.entries[0].Key, rightNum)
}

func (t *Tree[K, V]) splitInternal(path []pathEntry, nodeNum uint32, n *node[K, V]) error {
	mid := len(n.keys) / 2
	promoteKey := n.keys[mid]

	rightNum := t.allocPage()
	right := &node[K, V]{
		typ:      typeInternal,
		keys:     slices.Clone(n.keys[mid+1:]),
		children: slices.Clone(n.children[mid+1:]),
	}

	n.keys = slices.Clone(n.keys[:mid])
	n.children = slices.Clone(n.children[:mid+1])

	t.pages[rightNum] = right
	t.markDirty(rightNum)
	t.markDirty(nodeNum)

	return t.insertIntoParent(path, nodeNum, promoteKey, rightNum)
}

func (t *Tree[K, V]) insertIntoParent(path []pathEntry, leftNum uint32, key K, rightNum uint32) error {
	if len(path) == 0 {
		// Split the root — grow the tree by one level.
		newRootNum := t.allocPage()
		newRoot := &node[K, V]{
			typ:      typeInternal,
			keys:     []K{key},
			children: []uint32{leftNum, rightNum},
		}
		t.pages[newRootNum] = newRoot
		t.markDirty(newRootNum)
		t.meta.root = newRootNum
		t.meta.height++
		t.metaDirty = true
		return nil
	}

	pe := path[len(path)-1]
	parent, err := t.getNode(pe.page)
	if err != nil {
		return err
	}

	parent.keys = slices.Insert(parent.keys, pe.idx, key)
	parent.children = slices.Insert(parent.children, pe.idx+1, rightNum)
	t.markDirty(pe.page)

	if len(parent.keys) > t.maxInternal {
		return t.splitInternal(path[:len(path)-1], pe.page, parent)
	}
	return nil
}

// --- page cache / I/O --------------------------------------------------------

func (t *Tree[K, V]) getNode(pageNum uint32) (*node[K, V], error) {
	if n, ok := t.pages[pageNum]; ok {
		return n, nil
	}
	var buf [pageSize]byte
	if _, err := t.file.ReadAt(buf[:], int64(pageNum)*pageSize); err != nil {
		return nil, fmt.Errorf("btree: read page %d: %w", pageNum, err)
	}
	n, err := decodeNode(buf[:], &t.codec)
	if err != nil {
		return nil, fmt.Errorf("btree: decode page %d: %w", pageNum, err)
	}
	t.pages[pageNum] = n
	return n, nil
}

func (t *Tree[K, V]) markDirty(pageNum uint32) {
	t.dirty[pageNum] = true
}

func (t *Tree[K, V]) allocPage() uint32 {
	p := t.meta.nextPage
	t.meta.nextPage++
	t.metaDirty = true
	return p
}

func (t *Tree[K, V]) flush() error {
	for pageNum := range t.dirty {
		n := t.pages[pageNum]
		var buf [pageSize]byte
		encodeNode(n, &t.codec, buf[:])
		if _, err := t.file.WriteAt(buf[:], int64(pageNum)*pageSize); err != nil {
			return fmt.Errorf("btree: write page %d: %w", pageNum, err)
		}
	}
	if t.metaDirty {
		var buf [pageSize]byte
		encodeMeta(&t.meta, buf[:])
		if _, err := t.file.WriteAt(buf[:], 0); err != nil {
			return fmt.Errorf("btree: write meta: %w", err)
		}
	}
	clear(t.dirty)
	t.metaDirty = false
	return nil
}
