// Package linearhash provides a indexing backend for Kiwi key-value store
// using Linear Hasing based disk backed hash-table.
package linearhash

import (
	"os"
	"sync"
)

var defaultOptions = Options{
	ReadOnly: false,
	FileMode: os.ModePerm,
}

// Open opens the Linear Hashing index file and creates a Store instance.
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
	Fetch(offset uint64) ([]byte, error)
	Alloc(data []byte) (offset uint64, err error)
	Free(offset uint64) error
}