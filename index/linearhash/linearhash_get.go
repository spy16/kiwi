package linearhash

import "github.com/spy16/kiwi/index"

// Get finds the index entry for given key in the hash table and returns.
// If not entry found, returns ErrKeyNotFound.
func (idx *LinearHash) Get(key []byte) (*index.Entry, error) {
	hash := idx.hash(key)

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.getEntry(key, hash)
}

func (idx *LinearHash) getEntry(key []byte, hash uint64) (*index.Entry, error) {
	return nil, nil
}
