package bptree

import (
	"bytes"
	"sort"
)

type node struct {
	// temporary states
	dirty bool

	// node data
	id       int
	next     int
	entries  []entry
	children []int
}

func (n *node) SplitRight(rightID int) (right *node) {
	right = &node{id: rightID, dirty: true}
	size := len(n.entries)

	if n.IsLeaf() {
		at := (size - 1) / 2
		right.entries = make([]entry, size-at)
		copy(right.entries, n.entries[at:])
		n.entries = n.entries[:at]

		right.next = n.next // right node now points to the next of 'n'
		n.next = right.id   // left node points to 'right'
	} else {
		at := (size / 2) + 1
		right.entries = make([]entry, size-at)
		right.children = append([]int(nil), n.children[at:len(n.entries)+1]...)

		n.entries = n.entries[:at-1]
		n.children = n.children[:at]
	}

	// both nodes have changed and need to be written to file
	right.dirty = true
	n.dirty = true

	return right
}

func (n *node) InsertAt(idx int, e entry) {
	n.entries = append(n.entries, entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = e
	n.dirty = true
}

func (n *node) SetVal(entryIdx int, val uint64) {
	n.entries[entryIdx].val = val
	n.dirty = true
}

func (n node) Search(key []byte) (idx int, found bool) {
	L := len(n.entries)

	idx = sort.Search(L, func(i int) bool {
		return bytes.Compare(key, n.entries[i].key) != 1
	})
	found = (idx < L) && bytes.Equal(n.entries[idx].key, key)

	return idx, found
}

func (n node) IsLeaf() bool { return len(n.children) == 0 }

type entry struct {
	key []byte
	val uint64
}
