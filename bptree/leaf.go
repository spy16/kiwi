package bptree

import (
	"bytes"
)

type leafNode struct {
	tree *BPlusTree
	next *leafNode
	keys [][]byte
	vals [][]byte
}

func (node *leafNode) Get(key []byte) ([]byte, bool) {
	idx, found := binarySearch(node.keys, key)
	if !found {
		return nil, false
	}

	return node.vals[idx], true
}

func (node *leafNode) Put(key, val []byte) bool {
	idx, found := binarySearch(node.keys, key)
	if found {
		node.vals[idx] = val
		return false
	}
	node.insertEntry(idx, key, val)

	if node.tree.root.IsOverflow() {
		sibling := node.Split().(*leafNode)

		newRoot := &internalNode{
			tree:     node.tree,
			keys:     [][]byte{sibling.keys[0]},
			children: []bPlusNode{node, sibling},
		}
		node.tree.root = newRoot
	}

	return true
}

func (node *leafNode) Split() bPlusNode {
	at := (node.Size() - 1) / 2

	sibling := &leafNode{
		keys: make([][]byte, len(node.keys)-at),
		vals: make([][]byte, len(node.keys)-at),
		tree: node.tree,
		next: node.next,
	}

	copy(sibling.keys, node.keys[at:])
	copy(sibling.vals, node.vals[at:])

	node.keys = node.keys[:at]
	node.vals = node.vals[:at]
	node.next = sibling

	return sibling
}

func (node *leafNode) IsOverflow() bool {
	return len(node.vals) > node.tree.maxEntries
}

func (node *leafNode) Size() int {
	return len(node.keys)
}

func (node *leafNode) Key(idx int) []byte {
	return node.keys[idx]
}

func (node *leafNode) LeafKey() []byte {
	return node.Key(0)
}

func (node *leafNode) insertEntry(idx int, k, v []byte) {
	node.keys = append(node.keys, nil)
	node.vals = append(node.vals, nil)

	copy(node.keys[idx+1:], node.keys[idx:])
	copy(node.vals[idx+1:], node.vals[idx:])

	node.keys[idx] = k
	node.vals[idx] = v
}

func (node *leafNode) String() string {
	return nodeString(node)
}

func binarySearch(arr [][]byte, key []byte) (idx int, found bool) {
	lo, hi := 0, len(arr)-1
	mid := 0

	for lo <= hi {
		mid = (lo + hi) / 2

		switch bytes.Compare(arr[mid], key) {
		case 0:
			return mid, true

		case -1:
			lo = mid + 1

		case 1:
			hi = mid - 1
		}
	}

	return lo, false
}
