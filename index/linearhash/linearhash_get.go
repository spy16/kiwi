package linearhash

// Get finds the index entry for given key in the hash table and returns.
// If not entry found, returns ErrKeyNotFound.
func (idx *LinearHash) Get(key []byte) (uint64, error) {
	hash := idx.hash(key)

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.getEntry(key, hash)
}

func (idx *LinearHash) getEntry(key []byte, hash uint64) (uint64, error) {
	return 0, nil
}
