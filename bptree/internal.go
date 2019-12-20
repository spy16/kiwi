package bptree

type internalNode struct {
	tree     *BPlusTree
	keys     [][]byte
	children []bPlusNode
}

func (node *internalNode) Get(key []byte) ([]byte, bool) {
	idx, found := binarySearch(node.keys, key)
	if found {
		idx++
	}
	return node.children[idx].Get(key)
}

func (node *internalNode) Put(key, val []byte) bool {
	idx, _ := binarySearch(node.keys, key)
	target := node.children[idx]

	if isInsert := target.Put(key, val); !isInsert {
		return false
	}

	if target.IsOverflow() {
		sibling := target.Split()
		node.insertChild(sibling.LeafKey(), sibling)
	}

	if node.tree.root.IsOverflow() {
		sibling := node.Split()
		node.tree.root = &internalNode{
			tree:     node.tree,
			keys:     [][]byte{sibling.LeafKey()},
			children: []bPlusNode{node, sibling},
		}
	}

	return true
}

func (node *internalNode) Split() bPlusNode {
	at := (node.Size() / 2) + 1

	sibling := &internalNode{
		tree:     node.tree,
		keys:     append([][]byte(nil), node.keys[at:]...),
		children: append([]bPlusNode(nil), node.children[at:len(node.keys)+1]...),
	}

	node.keys = node.keys[:at-1]
	node.children = node.children[:at]

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

func (node *internalNode) LeafKey() []byte {
	return node.children[0].LeafKey()
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

func (node *internalNode) String() string {
	return nodeString(node)
}
