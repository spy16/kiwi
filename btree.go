package kiwi

import (
	"errors"
	"fmt"
	"strings"
)

// New returns an instance of empty B+ Tree of given order.
func New(order int) *BPlusTree {
	if order <= 2 {
		panic("order should be greater than 2")
	}

	mgr := &inMemoryNodeManager{}
	mgr.alloc() // allocate one node for the root

	return &BPlusTree{
		nodeManager: mgr,
		order:       order,
		rootid:      1,
		size:        0,
	}
}

// KVBytes is a convenience function to convert a string key value
// pair to a byte-slice key-value pair.
func KVBytes(k, v string) (key, val []byte) {
	return []byte(k), []byte(v)
}

// BPlusTree implements an order-m B+ Tree.
type BPlusTree struct {
	nodeManager

	order      int
	maxEntries int
	size       int
	rootid     nodeID
}

// Get searches for the given key and returns the value associated with it.
// Returns error if key not found.
func (tree *BPlusTree) Get(key []byte) ([]byte, error) {
	target, idx, found := tree.searchNodeRecursive(tree.root(), key)
	if !found {
		return nil, errors.New("key not found")
	}

	return target.vals[idx], nil
}

// Put inserts the key-value pair into the tree. If an entry with given key
// already exists, updates the value.
func (tree *BPlusTree) Put(key, val []byte) error {
	isInsert := tree.putEntry(tree.root(), key, val)
	if isInsert {
		tree.size++
	}

	return nil
}

// Delete removes an entry with the given key from the tree.
func (tree *BPlusTree) Delete(key []byte) error {
	isDelete := tree.delEntry(tree.root(), key)
	if !isDelete {
		return errors.New("key not found")
	}
	tree.size--

	return nil
}

// Size returns the number of items in the B+ Tree.
func (tree *BPlusTree) Size() int {
	return tree.size
}

func (tree *BPlusTree) String() string {
	return fmt.Sprintf("B+Tree{order=%d, size=%d}", tree.order, tree.size)
}

func (tree *BPlusTree) root() *node {
	return tree.node(tree.rootid)
}

// nodePut inserts/updates the given key-value pair into the node or its
// children. If the node is leaf, it will be inserted into it. If the node
// is internal, searc index will be used to choose the child where the entry
// should be added.
func (tree *BPlusTree) putEntry(node *node, key, val []byte) bool {
	idx, found := node.search(key)

	if node.isLeaf() {
		if found {
			node.vals[idx] = val
			return false
		}

		node.insertEntry(idx, key, val)
		tree.splitRootIfNeeded(node)
		return true
	}

	target := tree.node(node.children[idx])
	if isInsert := tree.putEntry(target, key, val); !isInsert {
		return false
	}

	if tree.isOverflow(target) {
		sibling := tree.split(target)
		node.addChild(tree.leafKey(sibling), sibling)
	}

	tree.splitRootIfNeeded(node)
	return true
}

func (tree *BPlusTree) delEntry(n *node, key []byte) bool {
	idx, found := n.search(key)

	if n.isLeaf() {
		if !found {
			return false
		}

		n.deleteEntry(idx)
		return true
	}

	child := tree.node(n.children[idx])
	if isDelete := tree.delEntry(child, key); !isDelete {
		return false
	}

	if tree.isUnderflow(child) {
		leftID, rightID := n.keySiblings(key)

		var left, right *node
		if leftID > 0 {
			left = tree.node(leftID)
			right = child
		} else {
			left = child
			right = tree.node(rightID)
		}

		tree.merge(left, right)
		child.next = right.next

		idx, found := right.search(tree.leafKey(right))
		if found {
			n.deleteEntry(idx)
			// TODO: Free node being removed.
			n.deleteChild(idx + 1)
		}

		if tree.isOverflow(left) {
			sibling := tree.split(left)
			n.addChild(tree.leafKey(sibling), sibling)
		}

		if len(tree.root().keys) == 0 {
			tree.rootid = left.id
		}
	}

	return true
}

func (tree *BPlusTree) merge(n *node, rightSibling *node) {
	if n.isLeaf() {
		if !rightSibling.isLeaf() {
			panic("can merge leaf with only leaf node")
		}

		n.keys = append(n.keys, rightSibling.keys...)
		n.vals = append(n.vals, rightSibling.vals...)
		n.next = rightSibling.id
		return
	}

	if rightSibling.isLeaf() {
		panic("can merge internal with only internal node")
	}

	n.keys = append(n.keys, tree.leafKey(n))
	n.keys = append(n.keys, rightSibling.keys...)
	n.children = append(n.children, rightSibling.children...)
}

func (tree *BPlusTree) splitRootIfNeeded(insertedIn *node) {
	if !tree.isOverflow(tree.root()) {
		return
	}

	sibling := tree.split(insertedIn)

	newRoot := tree.alloc()
	newRoot.keys = [][]byte{tree.leafKey(sibling)}
	newRoot.children = []nodeID{insertedIn.id, sibling.id}

	tree.rootid = newRoot.id
}

func (tree *BPlusTree) leafKey(n *node) []byte {
	if n.isLeaf() {
		return n.keys[0]
	}

	return tree.leafKey(tree.node(n.children[0]))
}

func (tree *BPlusTree) split(n *node) *node {
	sibling := tree.alloc()
	size := len(n.keys)

	if n.isLeaf() {
		at := (size - 1) / 2

		sibling.keys = make([][]byte, len(n.keys)-at)
		sibling.vals = make([][]byte, len(n.vals)-at)
		sibling.next = n.next

		copy(sibling.keys, n.keys[at:])
		copy(sibling.vals, n.vals[at:])

		n.keys = n.keys[:at]
		n.vals = n.vals[:at]
		n.next = sibling.id
	} else {
		at := (size / 2) + 1

		sibling.keys = append([][]byte(nil), n.keys[at:]...)
		sibling.children = append([]nodeID(nil), n.children[at:len(n.keys)+1]...)

		n.keys = n.keys[:at-1]
		n.children = n.children[:at]
	}

	return sibling
}

func (tree *BPlusTree) isOverflow(n *node) bool {
	if n.isLeaf() {
		return len(n.vals) > (tree.order - 1)
	}

	return len(n.children) > tree.order
}

func (tree *BPlusTree) isUnderflow(n *node) bool {
	if n.isLeaf() {
		return len(n.vals) < (tree.order / 2)
	}

	return len(n.children) < ((tree.order + 1) / 2)
}

func (tree *BPlusTree) searchNodeRecursive(n *node, key []byte) (target *node, idx int, found bool) {
	idx, found = n.search(key)
	if n.isLeaf() {
		return n, idx, found
	}

	if found {
		idx++
	}

	return tree.searchNodeRecursive(tree.node(n.children[idx]), key)
}

type nodeManager interface {
	alloc() *node
	node(id nodeID) *node
}

// Print formats and prints the tree.
func Print(tree *BPlusTree) {
	printNode(tree, tree.root(), 0)
}

func printNode(tree *BPlusTree, node *node, level int) {
	fmt.Printf("%s+ %s", strings.Repeat("--", level), node)

	if node.isLeaf() {
		fmt.Printf("     [-->%d]\n", node.next)
		return
	}

	fmt.Println()

	for i := 0; i < len(node.children); i++ {
		printNode(tree, tree.node(node.children[i]), level+1)
	}
}
