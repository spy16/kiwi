package bptree

import (
	"errors"
	"os"
)

// Put puts the key-value pair into the B+ tree. If the key already exists, its
// value will be updated.
func (tree *BPlusTree) Put(key []byte, val uint64) error {
	if len(key) > tree.keySize {
		return errors.New("key is too large")
	} else if len(key) == 0 {
		return errors.New("empty key")
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.pager == nil {
		return os.ErrClosed
	}

	if tree.root == nil {
		r, err := tree.fetch(tree.rootID)
		if err != nil {
			return err
		}
		tree.root = r
	}

	// maintain cache of all the nodes that were accessed.
	tree.nodes = map[int]*node{
		tree.root.id: tree.root,
	}

	e := entry{key: key, val: val}
	if err := tree.nodePut(tree.root, e); err != nil {
		return err
	}

	// write all the nodes that were modified/created
	return tree.writeAll()
}

func (tree *BPlusTree) nodePut(n *node, e entry) error {
	idx, found := n.Search(e.key)

	if n.IsLeaf() {
		if found {
			n.SetVal(idx, e.val)
			tree.size++
			return nil
		}

		n.InsertAt(idx, e)
		return tree.splitRootIfNeeded()
	}

	return nil
}

func (tree *BPlusTree) splitRootIfNeeded() error {
	if !tree.isOverflow(tree.root) {
		// splitting is not needed
		return nil
	}

	sibling, err := tree.split(tree.root)
	if err != nil {
		return err
	}

	leafKey, err := tree.leafKey(sibling)
	if err != nil {
		return err
	}

	id, err := tree.pager.Alloc(1)
	if err != nil {
		return err
	}

	newRoot := &node{
		id:       id,
		entries:  []entry{{key: leafKey}},
		children: []int{tree.root.id, sibling.id},
	}

	tree.rootID = id
	tree.root = newRoot
	tree.nodes[id] = newRoot

	return nil
}

func (tree *BPlusTree) split(n *node) (right *node, err error) {
	rightID, err := tree.pager.Alloc(1)
	if err != nil {
		return nil, err
	}

	right = n.SplitRight(rightID)
	tree.nodes[rightID] = right // queue for write
	return right, nil
}
