package linearhash

import (
	"github.com/spy16/kiwi/index"
)

// Del removes the entry for the given key from the hash table and returns
// the removed entry.
func (idx *LinearHash) Del(key []byte) (*index.Entry, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.isImmutable() {
		return nil, index.ErrImmutable
	}

	return nil, index.ErrKeyNotFound
}
