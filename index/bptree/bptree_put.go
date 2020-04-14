package bptree

import "github.com/spy16/kiwi/index"

// Put inserts the key value pair into the tree.
func (tree *BPlusTree) Put(entry index.Entry) error {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.isImmutable() {
		return index.ErrImmutable
	}

	return nil
}
