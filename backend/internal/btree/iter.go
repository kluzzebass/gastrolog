package btree

// Iter iterates over entries in key order.
// Use Valid to check position, Key/Value to read, Next to advance.
type Iter[K, V any] struct {
	tree    *Tree[K, V]
	leaf    *node[K, V]
	leafNum uint32
	pos     int
	err     error
}

// Valid reports whether the iterator points to a valid entry.
func (it *Iter[K, V]) Valid() bool {
	return it.leaf != nil && it.pos < len(it.leaf.entries)
}

// Key returns the current entry's key.
func (it *Iter[K, V]) Key() K { return it.leaf.entries[it.pos].Key }

// Value returns the current entry's value.
func (it *Iter[K, V]) Value() V { return it.leaf.entries[it.pos].Value }

// Entry returns the current entry.
func (it *Iter[K, V]) Entry() Entry[K, V] { return it.leaf.entries[it.pos] }

// Err returns the first I/O error encountered during iteration, if any.
func (it *Iter[K, V]) Err() error { return it.err }

// Next advances the iterator to the next entry.
func (it *Iter[K, V]) Next() {
	if !it.Valid() {
		return
	}
	it.pos++
	if it.pos < len(it.leaf.entries) {
		return
	}
	nextNum := it.leaf.nextLeaf
	if nextNum == 0 {
		it.leaf = nil
		return
	}
	next, err := it.tree.getNode(nextNum)
	if err != nil {
		it.leaf = nil
		it.err = err
		return
	}
	it.leaf = next
	it.leafNum = nextNum
	it.pos = 0
}
