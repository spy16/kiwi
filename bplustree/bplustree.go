package bplustree

import (
	"bytes"
	"errors"
)

type BPlusTree struct {
	root *node
	size int

	leafDegree int
	degree     int

	nextID int
	nodes  map[int]*node
}

func (tree *BPlusTree) Put(key []byte, val uint64) error {
	if tree.root == nil {
		r, err := tree.allocOne()
		if err != nil {
			return nil
		}
		tree.root = r
	}

	isInsert, err := tree.put(entry{key: key, val: val})
	if err != nil {
		return err
	}

	if isInsert {
		tree.size++
	}

	return nil
}

func (tree *BPlusTree) put(e entry) (bool, error) {
	if tree.isFull(tree.root) {
		oldRoot := tree.root
		newRoot, err := tree.allocOne()
		if err != nil {
			return false, err
		}
		newRoot.children = append(newRoot.children, oldRoot.id)
		tree.root = newRoot

		if err := tree.split(newRoot, oldRoot, 0); err != nil {
			return false, err
		}

		return tree.insertNonFull(newRoot, e)
	}

	return tree.insertNonFull(tree.root, e)
}

func (tree *BPlusTree) insertNonFull(n *node, e entry) (bool, error) {
	idx, found := n.search(e.key)

	if len(n.children) == 0 {
		// a leaf node
		if found {
			// key already exists
			n.update(idx, e.val)
			return false, nil
		}
		n.insert(idx, e)
		return true, nil
	}

	child, err := tree.fetch(n.children[idx])
	if err != nil {
		return false, err
	}

	if tree.isFull(child) {
		if err := tree.split(n, child, idx); err != nil {
			return false, err
		}

		// should go into left child or right child?
		if bytes.Compare(e.key, n.entries[idx].key) >= 0 {
			child, err = tree.fetch(n.children[idx+1])
			if err != nil {
				return false, err
			}
		}
	}

	return tree.insertNonFull(child, e)
}

func (tree *BPlusTree) split(p, n *node, i int) error {
	sibling, err := tree.allocOne()
	if err != nil {
		return err
	}

	if len(n.children) == 0 {
		// leaf node

		// use sibling as the right node for 'n'
		sibling.next = n.next
		n.next = sibling.id

		sibling.entries = make([]entry, tree.leafDegree-1)
		copy(sibling.entries, n.entries[tree.leafDegree:])
		n.entries = n.entries[:tree.leafDegree]

		p.insertChild(i+1, sibling.id)
		p.insert(i, sibling.entries[0])

		return nil
	}

	// internal node
	// use sibling as left node for 'n'
	parentKey := n.entries[tree.degree-1]

	sibling.entries = make([]entry, tree.degree-1)
	copy(sibling.entries, n.entries[:tree.degree])
	n.entries = n.entries[tree.degree:]

	sibling.children = make([]int, tree.degree)
	copy(sibling.children, n.children[:tree.degree])
	n.children = n.children[tree.degree:]

	p.insertChild(i, sibling.id)
	p.insert(i, parentKey)

	return nil
}

func (tree *BPlusTree) isFull(n *node) bool {
	if len(n.children) == 0 { // leaf node
		return len(n.entries) == ((2 * tree.leafDegree) - 1)
	}
	return len(n.entries) == ((2 * tree.degree) - 1)
}

func (tree *BPlusTree) fetch(id int) (*node, error) {
	n, found := tree.nodes[id]
	if !found {
		return nil, errors.New("no such node")
	}
	return n, nil
}

func (tree *BPlusTree) allocOne() (*node, error) {
	nodes, err := tree.alloc(1)
	if err != nil {
		return nil, err
	}
	return nodes[0], nil
}

func (tree *BPlusTree) alloc(n int) ([]*node, error) {
	nodes := make([]*node, n)

	for i := 0; i < n; i++ {
		id := tree.nextID
		tree.nextID++

		nodes[i] = &node{
			dirty: true,
			id:    id,
		}
		tree.nodes[id] = nodes[i]
	}

	return nodes, nil
}
