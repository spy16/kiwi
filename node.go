package kiwi

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var errNotFound = errors.New("key not found")

type inMemoryNodeManager struct {
	nodes    []*node
	freelist []nodeID
}

func (im *inMemoryNodeManager) alloc() *node {
	if len(im.freelist) > 0 {
		id := im.freelist[0]
		im.freelist = im.freelist[1:]
		return im.node(id)
	}

	n := &node{}
	n.id = im.nextID()
	im.nodes = append(im.nodes, n)
	return n
}

func (im *inMemoryNodeManager) free(id nodeID) {
	if int(id) > len(im.nodes) {
		return
	}

	im.freelist = append(im.freelist, id)
}

func (im *inMemoryNodeManager) node(id nodeID) *node {
	return im.nodes[int(id)-1]
}

func (im *inMemoryNodeManager) nextID() nodeID {
	return nodeID(len(im.nodes) + 1)
}

type nodeID uint64

type node struct {
	id       nodeID
	next     nodeID
	keys     [][]byte
	vals     [][]byte
	children []nodeID
}

func (n *node) search(key []byte) (idx int, found bool) {
	L := len(n.keys)

	idx = sort.Search(L, func(i int) bool {
		return bytes.Compare(key, n.keys[i]) != 1
	})
	found = (idx < L) && bytes.Equal(n.keys[idx], key)

	return idx, found
}

func (n *node) keySiblings(key []byte) (left, right nodeID) {
	idx, _ := n.search(key)

	if idx > 0 {
		left = n.children[idx-1]
	}

	if idx < len(n.keys) {
		right = n.children[idx+1]
	}

	return left, right
}

func (n *node) addChild(key []byte, child *node) {
	idx, found := n.search(key)
	if found {
		n.children[idx] = child.id
		return
	}

	n.insertKey(idx, key)
	n.insertChild(idx+1, child)
}

func (n *node) insertEntry(idx int, key, val []byte) {
	n.insertKey(idx, key)
	if n.isLeaf() {
		n.insertVal(idx, val)
	}
}

func (n *node) deleteEntry(idx int) {
	n.keys[idx] = nil
	n.keys = append(n.keys[:idx], n.keys[idx+1:]...)

	if n.isLeaf() {
		n.vals[idx] = nil
		n.vals = append(n.vals[:idx], n.vals[idx+1:]...)
	}
}

func (n *node) insertChild(idx int, child *node) {
	n.children = append(n.children, 0)
	copy(n.children[idx+1:], n.children[idx:])
	n.children[idx] = child.id
}

func (n *node) deleteChild(idx int) {
	n.children = append(n.children[:idx], n.children[idx+1:]...)
}

func (n *node) insertKey(idx int, key []byte) {
	n.keys = append(n.keys, nil)
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = key
}

func (n *node) insertVal(idx int, val []byte) {
	n.vals = append(n.vals, nil)
	copy(n.vals[idx+1:], n.vals[idx:])
	n.vals[idx] = val
}

func (n *node) isLeaf() bool {
	return len(n.children) == 0
}

func (n *node) String() string {
	kind := "Internal"
	if n.isLeaf() {
		kind = "Leaf"
	}

	nextNode := "x"
	if n.isLeaf() && n.next != 0 {
		nextNode = fmt.Sprintf("%d", n.next)
	}

	return fmt.Sprintf("%s{id=%d keys=%s next=%s}", kind, n.id, keysStr(n.keys), nextNode)
}

func keysStr(keys [][]byte) string {
	var parts []string
	for _, key := range keys {
		parts = append(parts, string(key))
	}

	return "[" + strings.Join(parts, ",") + "]"
}
