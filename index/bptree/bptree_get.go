package bptree

import "github.com/spy16/kiwi/index"

// Get returns the indexing entry for the given key. If entry for the
// given key doesn't exist, then returns ErrKeyNotFound.
func (tree *BPlusTree) Get(key []byte) (*index.Entry, error) {
	return nil, index.ErrKeyNotFound
}
