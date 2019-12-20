package bptree

import "log"

type internalNode struct {
	tree     *BPlusTree
	keys     [][]byte
	children []bPlusNode
}

func (node *internalNode) Get(key []byte) ([]byte, bool) {
	idx, _ := binarySearch(node.keys, key)
	return node.children[idx].Get(key)
}

func (node *internalNode) Put(key, val []byte) bool {
	idx, _ := binarySearch(node.keys, key)
	target := node.children[idx]

	if isInsert := target.Put(key, val); !isInsert {
		return false
	}

	log.Printf("target=%s", nodeString(target))
	if target.IsOverflow() {
		sibling := target.Split()
		node.insertChild(sibling.Key(0), sibling)
	}

	if node.tree.root.IsOverflow() {
		sibling := node.Split()
		newRoot := &internalNode{
			tree:     node.tree,
			keys:     [][]byte{sibling.Key(0)},
			children: []bPlusNode{node, sibling},
		}
		node.tree.root = newRoot
	}

	return true
}

func (node *internalNode) Split() bPlusNode {
	at := (node.Size() - 1) / 2

	sibling := &internalNode{
		tree:     node.tree,
		keys:     append([][]byte(nil), node.keys[at+1:]...),
		children: append([]bPlusNode(nil), node.children[at:len(node.keys)]...),
	}

	node.keys = node.keys[at-1 : node.Size()]
	node.children = node.children[at : node.Size()+1]

	return sibling
}

func (node *internalNode) insertChild(key []byte, child bPlusNode) {
	idx, found := binarySearch(node.keys, key)
	if found {
		node.children[idx] = child
		return
	}

	node.keys = append(node.keys, nil)
	copy(node.keys[idx+1:], node.keys[idx:])
	node.keys[idx] = key

	childIdx := idx + 1
	node.children = append(node.children, nil)
	copy(node.children[childIdx+1:], node.children[childIdx:])
	node.children[childIdx] = child
}

func (node *internalNode) IsOverflow() bool {
	return len(node.children) > node.tree.order
}

func (node *internalNode) Key(idx int) []byte {
	return node.keys[idx]
}

func (node *internalNode) Size() int {
	return len(node.keys)
}
