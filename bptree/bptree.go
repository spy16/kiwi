package bptree

import (
	"errors"
	"fmt"
	"strings"
)

// New initializes an empty B+ Tree of given order.
func New(order int) *BPlusTree {
	tree := &BPlusTree{
		order:      order,
		maxEntries: order - 1,
	}

	tree.root = &leafNode{
		tree: tree,
	}

	return tree
}

// BPlusTree implements an order-m B+ Tree.
type BPlusTree struct {
	order      int
	maxEntries int
	size       int
	root       bPlusNode
}

// Get searches for the given key and returns the value associated
// with it. Returns error if key not found.
func (tree *BPlusTree) Get(key []byte) ([]byte, error) {
	v, found := tree.root.Get(key)
	if !found {
		return nil, errors.New("key not found")
	}

	return v, nil
}

// Put inserts the key-value pair into the tree. If an entry with given
// key already exists, updates the value.
func (tree *BPlusTree) Put(k, v []byte) error {
	isInsert := tree.root.Put(k, v)
	if isInsert {
		tree.size++
	}

	return nil
}

// Print formats and prints the tree.
func Print(tree *BPlusTree) {
	printNode(tree.root, 0)
}

func printNode(node bPlusNode, level int) {
	fmt.Printf("%s+ %s\n", strings.Repeat("--", level), nodeString(node))

	n, ok := node.(*internalNode)
	if !ok {
		return
	}

	for i := 0; i < len(n.children); i++ {
		printNode(n.children[i], level+1)
	}
}

func nodeString(node bPlusNode) string {
	var keys []string
	for i := 0; i < node.Size(); i++ {
		keys = append(keys, string(node.Key(i)))
	}

	typ := "Leaf"
	if _, ok := node.(*internalNode); ok {
		typ = "Internal"
	}

	return typ + "{" + strings.Join(keys, ", ") + "}"
}

type bPlusNode interface {
	Put(k, v []byte) bool
	Get(k []byte) ([]byte, bool)
	IsOverflow() bool
	Split() bPlusNode
	Key(idx int) []byte
	Size() int
}
