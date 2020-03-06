// Package linearhash provides a indexing backend for Kiwi key-value store
// using Linear Hasing based disk backed hash-table.
package linearhash

import (
	"errors"
	"os"
	"sync"
)

var (
	// ErrNotFound is returned when a key is not found.
	ErrNotFound = errors.New("key not found")

	// ErrEmptyKey is returned when a key is zero sized.
	ErrEmptyKey = errors.New("invalid sized key")

	// ErrOpNotAllowed is returned when write operation (put/del)
	// is attempted on a readonly.
	ErrOpNotAllowed = errors.New("operation not allowed in read-only mode")
)

var defaultOptions = Options{
	ReadOnly: false,
	FileMode: os.ModePerm,
}

// Open opens the Linear Hashing index file and creates an Store instance.
// If nil value is provided for options, defaultOptions are used.
func Open(indexFile string, store BlobStore, opts *Options) (*Store, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	fh, size, err := openFile(indexFile, opts.FileMode, opts.ReadOnly)
	if err != nil {
		return nil, err
	}

	lhs := &Store{
		mu:       &sync.RWMutex{},
		size:     size,
		index:    fh,
		blobs:    store,
		readOnly: opts.ReadOnly,
	}

	if err := lhs.init(); err != nil {
		lhs.Close()
		return nil, err
	}

	if err := lhs.mmapFile(); err != nil {
		lhs.Close()
		return nil, err
	}

	return lhs, nil
}

// Options represents configuration options for LHStore.
type Options struct {
	ReadOnly bool
	FileMode os.FileMode
}

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
