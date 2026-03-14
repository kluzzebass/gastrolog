// Package btree implements a file-backed B+ tree.
//
// The tree maps keys to values. Key ordering is defined by the codec's Compare
// function. Duplicate keys are allowed; entries with equal keys are stored in
// insertion order.
//
// Uses pread/pwrite for I/O with an in-process page cache.
// Not safe for concurrent use; callers must synchronize access.
package btree

import (
	"fmt"
	"os"
	"slices"
)

// Tree is a file-backed B+ tree.
type Tree[K, V any] struct {
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
func Create[K, V any](path string, codec Codec[K, V]) (*Tree[K, V], error) {
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
func Open[K, V any](path string, codec Codec[K, V]) (*Tree[K, V], error) {
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
			// Use a comparator that treats equal keys as "less than" the
			// target so the insertion point falls after all existing entries
			// with the same key, preserving insertion order for duplicates.
			pos, _ := slices.BinarySearchFunc(n.entries, e, func(a, b Entry[K, V]) int {
				if c := t.codec.Compare(a.Key, b.Key); c != 0 {
					return c
				}
				return -1
			})
			n.entries = slices.Insert(n.entries, pos, e)
			t.markDirty(pageNum)
			t.meta.count++
			t.metaDirty = true

			if len(n.entries) > t.maxLeaf {
				return t.splitLeaf(path, pageNum, n)
			}
			return nil
		}

		childIdx := t.findChild(n.keys, key)
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
		pageNum = n.children[t.findChild(n.keys, key)]
	}
}

func (t *Tree[K, V]) findInLeaf(leaf *node[K, V], leafNum uint32, key K) (*Iter[K, V], error) {
	idx, _ := slices.BinarySearchFunc(leaf.entries, key, func(e Entry[K, V], target K) int {
		return t.codec.Compare(e.Key, target)
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

// deleteFrame tracks a parent during tree descent for delete rebalancing.
type deleteFrame struct {
	page     uint32
	childIdx int // index into children that we descended into
}

// Delete removes the first entry with the given key.
// Returns (true, nil) if the key was found and removed, (false, nil) if not found.
func (t *Tree[K, V]) Delete(key K) (bool, error) {
	path := make([]deleteFrame, 0, t.meta.height)
	pageNum := t.meta.root

	for {
		n, err := t.getNode(pageNum)
		if err != nil {
			return false, err
		}
		if n.typ == typeLeaf {
			// Binary search for the key.
			idx, found := slices.BinarySearchFunc(n.entries, key, func(e Entry[K, V], target K) int {
				return t.codec.Compare(e.Key, target)
			})
			if !found {
				return false, nil
			}
			// Remove the entry.
			n.entries = slices.Delete(n.entries, idx, idx+1)
			t.markDirty(pageNum)
			t.meta.count--
			t.metaDirty = true

			// Rebalance if underflow (unless this is the root).
			minLeaf := t.maxLeaf / 2
			if len(n.entries) < minLeaf && len(path) > 0 {
				return true, t.rebalanceLeaf(path, pageNum, n)
			}
			return true, nil
		}
		childIdx := t.findChild(n.keys, key)
		path = append(path, deleteFrame{page: pageNum, childIdx: childIdx})
		pageNum = n.children[childIdx]
	}
}

// rebalanceLeaf rebalances an underflowing leaf by redistributing from a
// sibling or merging with one.
func (t *Tree[K, V]) rebalanceLeaf(path []deleteFrame, leafNum uint32, leaf *node[K, V]) error {
	parent := path[len(path)-1]
	pNode, err := t.getNode(parent.page)
	if err != nil {
		return err
	}

	// Try left sibling.
	if parent.childIdx > 0 {
		return t.rebalanceLeafLeft(path, parent, pNode, leafNum, leaf)
	}

	// Try right sibling.
	if parent.childIdx < len(pNode.children)-1 {
		return t.rebalanceLeafRight(path, parent, pNode, leafNum, leaf)
	}

	return nil
}

func (t *Tree[K, V]) rebalanceLeafLeft(path []deleteFrame, parent deleteFrame, pNode *node[K, V], leafNum uint32, leaf *node[K, V]) error {
	leftNum := pNode.children[parent.childIdx-1]
	left, err := t.getNode(leftNum)
	if err != nil {
		return err
	}
	if len(left.entries) > t.maxLeaf/2 {
		// Redistribute: move last entry from left sibling to this leaf.
		donor := left.entries[len(left.entries)-1]
		left.entries = left.entries[:len(left.entries)-1]
		leaf.entries = slices.Insert(leaf.entries, 0, donor)
		pNode.keys[parent.childIdx-1] = leaf.entries[0].Key
		t.markDirty(leftNum)
		t.markDirty(leafNum)
		t.markDirty(parent.page)
		return nil
	}
	// Merge into left sibling.
	left.entries = append(left.entries, leaf.entries...)
	left.nextLeaf = leaf.nextLeaf
	if err := t.updatePrevLeaf(leaf.nextLeaf, leftNum); err != nil {
		return err
	}
	t.markDirty(leftNum)
	delete(t.pages, leafNum)
	delete(t.dirty, leafNum)
	pNode.keys = slices.Delete(pNode.keys, parent.childIdx-1, parent.childIdx)
	pNode.children = slices.Delete(pNode.children, parent.childIdx, parent.childIdx+1)
	t.markDirty(parent.page)
	return t.rebalanceInternal(path[:len(path)-1], parent.page, pNode)
}

func (t *Tree[K, V]) rebalanceLeafRight(path []deleteFrame, parent deleteFrame, pNode *node[K, V], leafNum uint32, leaf *node[K, V]) error {
	rightNum := pNode.children[parent.childIdx+1]
	right, err := t.getNode(rightNum)
	if err != nil {
		return err
	}
	if len(right.entries) > t.maxLeaf/2 {
		// Redistribute: move first entry from right sibling to this leaf.
		donor := right.entries[0]
		right.entries = slices.Delete(right.entries, 0, 1)
		leaf.entries = append(leaf.entries, donor)
		pNode.keys[parent.childIdx] = right.entries[0].Key
		t.markDirty(rightNum)
		t.markDirty(leafNum)
		t.markDirty(parent.page)
		return nil
	}
	// Merge right into this leaf.
	leaf.entries = append(leaf.entries, right.entries...)
	leaf.nextLeaf = right.nextLeaf
	if err := t.updatePrevLeaf(right.nextLeaf, leafNum); err != nil {
		return err
	}
	t.markDirty(leafNum)
	delete(t.pages, rightNum)
	delete(t.dirty, rightNum)
	pNode.keys = slices.Delete(pNode.keys, parent.childIdx, parent.childIdx+1)
	pNode.children = slices.Delete(pNode.children, parent.childIdx+1, parent.childIdx+2)
	t.markDirty(parent.page)
	return t.rebalanceInternal(path[:len(path)-1], parent.page, pNode)
}

// updatePrevLeaf updates the prevLeaf pointer of the successor leaf node.
func (t *Tree[K, V]) updatePrevLeaf(succNum uint32, newPrev uint32) error {
	if succNum == 0 {
		return nil
	}
	succ, err := t.getNode(succNum)
	if err != nil {
		return err
	}
	succ.prevLeaf = newPrev
	t.markDirty(succNum)
	return nil
}

// rebalanceInternal rebalances an underflowing internal node.
func (t *Tree[K, V]) rebalanceInternal(path []deleteFrame, nodeNum uint32, n *node[K, V]) error {
	// If this is the root and it has no keys, shrink the tree.
	if len(path) == 0 {
		if len(n.keys) == 0 && len(n.children) == 1 {
			t.meta.root = n.children[0]
			t.meta.height--
			t.metaDirty = true
			delete(t.pages, nodeNum)
			delete(t.dirty, nodeNum)
		}
		return nil
	}

	minInternal := t.maxInternal / 2
	if len(n.keys) >= minInternal {
		return nil
	}

	parent := path[len(path)-1]
	pNode, err := t.getNode(parent.page)
	if err != nil {
		return err
	}

	if parent.childIdx > 0 {
		return t.rebalanceInternalLeft(path, parent, pNode, nodeNum, n)
	}
	if parent.childIdx < len(pNode.children)-1 {
		return t.rebalanceInternalRight(path, parent, pNode, nodeNum, n)
	}
	return nil
}

func (t *Tree[K, V]) rebalanceInternalLeft(path []deleteFrame, parent deleteFrame, pNode *node[K, V], nodeNum uint32, n *node[K, V]) error {
	leftNum := pNode.children[parent.childIdx-1]
	left, err := t.getNode(leftNum)
	if err != nil {
		return err
	}
	minInternal := t.maxInternal / 2
	if len(left.keys) > minInternal {
		// Redistribute: rotate right through parent.
		n.keys = slices.Insert(n.keys, 0, pNode.keys[parent.childIdx-1])
		n.children = slices.Insert(n.children, 0, left.children[len(left.children)-1])
		pNode.keys[parent.childIdx-1] = left.keys[len(left.keys)-1]
		left.keys = left.keys[:len(left.keys)-1]
		left.children = left.children[:len(left.children)-1]
		t.markDirty(leftNum)
		t.markDirty(nodeNum)
		t.markDirty(parent.page)
		return nil
	}
	// Merge into left sibling: left + separator + this node.
	left.keys = append(left.keys, pNode.keys[parent.childIdx-1])
	left.keys = append(left.keys, n.keys...)
	left.children = append(left.children, n.children...)
	t.markDirty(leftNum)
	delete(t.pages, nodeNum)
	delete(t.dirty, nodeNum)
	pNode.keys = slices.Delete(pNode.keys, parent.childIdx-1, parent.childIdx)
	pNode.children = slices.Delete(pNode.children, parent.childIdx, parent.childIdx+1)
	t.markDirty(parent.page)
	return t.rebalanceInternal(path[:len(path)-1], parent.page, pNode)
}

func (t *Tree[K, V]) rebalanceInternalRight(path []deleteFrame, parent deleteFrame, pNode *node[K, V], nodeNum uint32, n *node[K, V]) error {
	rightNum := pNode.children[parent.childIdx+1]
	right, err := t.getNode(rightNum)
	if err != nil {
		return err
	}
	minInternal := t.maxInternal / 2
	if len(right.keys) > minInternal {
		// Redistribute: rotate left through parent.
		n.keys = append(n.keys, pNode.keys[parent.childIdx])
		n.children = append(n.children, right.children[0])
		pNode.keys[parent.childIdx] = right.keys[0]
		right.keys = slices.Delete(right.keys, 0, 1)
		right.children = slices.Delete(right.children, 0, 1)
		t.markDirty(rightNum)
		t.markDirty(nodeNum)
		t.markDirty(parent.page)
		return nil
	}
	// Merge right into this node: this + separator + right.
	n.keys = append(n.keys, pNode.keys[parent.childIdx])
	n.keys = append(n.keys, right.keys...)
	n.children = append(n.children, right.children...)
	t.markDirty(nodeNum)
	delete(t.pages, rightNum)
	delete(t.dirty, rightNum)
	pNode.keys = slices.Delete(pNode.keys, parent.childIdx, parent.childIdx+1)
	pNode.children = slices.Delete(pNode.children, parent.childIdx+1, parent.childIdx+2)
	t.markDirty(parent.page)
	return t.rebalanceInternal(path[:len(path)-1], parent.page, pNode)
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
func (t *Tree[K, V]) findChild(keys []K, key K) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if t.codec.Compare(keys[mid], key) <= 0 {
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
