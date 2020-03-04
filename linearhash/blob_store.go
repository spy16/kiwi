package linearhash

import (
	"fmt"
	"sync"
)

var _ BlobStore = (*InMemoryBlobStore)(nil)

// BlobStore implementation is responsible for storing binary blobs and
// managing offsets & free-lists.
type BlobStore interface {
	// Fetch should return the binary blob starting at given offset.
	// Size of the blob should be handled by the blob store.
	Fetch(offset uint64) ([]byte, error)

	// Alloc should allocate space & store the given binary data and
	// return the allocated offset.
	Alloc(data []byte) (offset uint64, err error)

	// Free should de-allocate the binary blob starting at given offset.
	// Size of the blob should be handled by the blob store.
	Free(offset uint64) error
}

// InMemoryBlobStore implements a binary store using in-memory arrays.
type InMemoryBlobStore struct {
	initOnce sync.Once
	mu       *sync.RWMutex
	blobs    []blob
}

// Alloc creates a blob, appends it to the blobs list and returns the
// array index of the blob.
func (inmem *InMemoryBlobStore) Alloc(data []byte) (uint64, error) {
	inmem.initOnce.Do(func() {
		inmem.mu = &sync.RWMutex{}
	})
	inmem.mu.Lock()
	defer inmem.mu.Unlock()

	inmem.blobs = append(inmem.blobs, blob{
		data: data,
		size: len(data),
	})
	return uint64(len(inmem.blobs) - 1), nil
}

// Fetch returns the blob stored at given index/offset. Returns error
// if the index is not found.
func (inmem *InMemoryBlobStore) Fetch(offset uint64) ([]byte, error) {
	if int(offset) >= len(inmem.blobs) {
		return nil, fmt.Errorf("invalid offset, valid range [0, %d)", len(inmem.blobs))
	}
	inmem.mu.RLock()
	defer inmem.mu.RUnlock()

	return inmem.blobs[offset].data, nil
}

// Free removes the entry at given index/offset. Returns error if the
// offset is invalid.
func (inmem *InMemoryBlobStore) Free(offset uint64) error {
	if int(offset) >= len(inmem.blobs) {
		return fmt.Errorf("invalid offset, valid range [0, %d)", len(inmem.blobs))
	}

	inmem.mu.Lock()
	defer inmem.mu.Unlock()

	inmem.blobs = append(inmem.blobs[:offset], inmem.blobs[offset+1:]...)
	return nil
}

type blob struct {
	size int
	data []byte
}
