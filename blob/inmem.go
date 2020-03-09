package blob

import (
	"fmt"
	"sync"
)

// InMemStore implements a concurrent safe binary store using in-memory arrays.
// Zero value is ready to use.
type InMemStore struct {
	initOnce sync.Once
	mu       *sync.RWMutex
	blobs    [][]byte
	freeList []uint64
}

// Alloc creates a blob, appends it to the blobs list and returns the
// array index of the blob.
func (inmem *InMemStore) Alloc(data []byte) (uint64, error) {
	inmem.initOnce.Do(func() {
		inmem.mu = &sync.RWMutex{}
	})
	inmem.mu.Lock()
	defer inmem.mu.Unlock()

	if len(inmem.freeList) > 0 {
		first := inmem.freeList[0]
		inmem.freeList = inmem.freeList[1:]
		inmem.blobs[first] = data
		return first, nil
	}

	inmem.blobs = append(inmem.blobs, data)
	return uint64(len(inmem.blobs) - 1), nil
}

// Fetch returns the blob stored at given index/offset. Returns error
// if the index is not found.
func (inmem *InMemStore) Fetch(offset uint64) ([]byte, error) {
	if int(offset) >= len(inmem.blobs) {
		return nil, fmt.Errorf("invalid offset, valid range [0, %d)", len(inmem.blobs))
	}
	inmem.mu.RLock()
	defer inmem.mu.RUnlock()

	return inmem.blobs[offset], nil
}

// Free removes the entry at given index/offset. Returns error if the
// offset is invalid.
func (inmem *InMemStore) Free(offset uint64) error {
	if int(offset) >= len(inmem.blobs) {
		return fmt.Errorf("invalid offset, valid range [0, %d)", len(inmem.blobs))
	}

	inmem.mu.Lock()
	defer inmem.mu.Unlock()

	if inmem.isFree(offset) {
		return nil // already free, nothing to do
	}

	inmem.freeList = append(inmem.freeList, offset)
	inmem.blobs[offset] = nil
	return nil
}

func (inmem *InMemStore) isFree(offset uint64) bool {
	for _, freeOffset := range inmem.freeList {
		if offset == freeOffset {
			return true
		}
	}
	return false
}
