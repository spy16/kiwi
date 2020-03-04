package btree

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
	freelist []nodeid
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

func (im *inMemoryNodeManager) free(id nodeid) {
	if int(id) > len(im.nodes) {
		return
	}

	im.freelist = append(im.freelist, id)
}

func (im *inMemoryNodeManager) node(id nodeid) *node {
	return im.nodes[int(id)-1]
}

func (im *inMemoryNodeManager) nextID() nodeid {
	return nodeid(len(im.nodes) + 1)
}

type nodeid uint64

type node struct {
	id       nodeid
	next     nodeid
	keys     [][]byte
	vals     [][]byte
	children []nodeid
}

func (n *node) String() string {
	var keys []string
	for _, key := range n.keys {
		keys = append(keys, string(key))
	}

	nodeType := "Internal"
	if n.isLeaf() {
		nodeType = "Leaf"
	}

	nextNode := "x"
	if n.isLeaf() && n.next != 0 {
		nextNode = fmt.Sprintf("%d", n.next)
	}

	return fmt.Sprintf("%s{id=%d keys=%s next=%s}",
		nodeType, n.id, "["+strings.Join(keys, ",")+"]", nextNode)
}

func (n *node) search(key []byte) (idx int, found bool) {
	L := len(n.keys)

	idx = sort.Search(L, func(i int) bool {
		return bytes.Compare(key, n.keys[i]) != 1
	})
	found = (idx < L) && bytes.Equal(n.keys[idx], key)

	return idx, found
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

func (n *node) insertChild(idx int, child *node) {
	n.children = append(n.children, 0)
	copy(n.children[idx+1:], n.children[idx:])
	n.children[idx] = child.id
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
