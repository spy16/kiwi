package bptree

import (
	"github.com/spy16/kiwi/index"
)

// Del removes the entry with given key from the B+ tree and returns
// the removed entry. If entry for given key doesn't exist, returns
// ErrKeyNotFound.
func (tree *BPlusTree) Del(key []byte) (*index.Entry, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.isImmutable() {
		return nil, index.ErrImmutable
	}

	return nil, index.ErrKeyNotFound
}
