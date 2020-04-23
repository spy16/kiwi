package bplustree

import (
	"bytes"
	"fmt"
	"sort"
)

type node struct {
	// temporary state
	dirty bool

	// actual node data
	id       int
	next     int
	entries  []entry
	children []int
}

func (n node) String() string {
	s := "{ "
	for _, e := range n.entries {
		s += fmt.Sprintf("'%s' ", e.key)
	}
	s += "}"

	return s
}

func (n *node) insert(idx int, e entry) {
	n.dirty = true
	n.entries = append(n.entries, entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = e
}

func (n *node) insertChild(idx int, childID int) {
	n.dirty = true
	n.children = append(n.children, 0)
	copy(n.children[idx+1:], n.children[idx:])
	n.children[idx] = childID
}

func (n *node) update(idx int, val uint64) {
	n.entries[idx].val = val
	n.dirty = true
}

func (n node) search(key []byte) (idx int, found bool) {
	L := len(n.entries)

	idx = sort.Search(L, func(i int) bool {
		return bytes.Compare(key, n.entries[i].key) != 1
	})
	found = (idx < L) && bytes.Equal(n.entries[idx].key, key)

	return idx, found
}

func (n node) isLeaf() bool { return len(n.children) == 0 }

type entry struct {
	key []byte
	val uint64
}
