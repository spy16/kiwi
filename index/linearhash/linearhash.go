package linearhash

import (
	"errors"
	"fmt"
	"hash/maphash"
	"os"
	"sync"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/io"
)

// Open opens the file as linear-hash indexing file and returns the
// indexer instance. If 'opts' is nil, uses default options.
func Open(indexFile string, blobs BlobStore, opts *Options) (*LinearHash, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	flag := os.O_CREATE | os.O_RDWR
	if opts.ReadOnly {
		flag = os.O_RDONLY
	}

	fh, err := io.OpenFile(indexFile, flag, opts.FileMode)
	if err != nil {
		return nil, err
	}

	idx := &LinearHash{
		mu:       &sync.RWMutex{},
		file:     fh,
		blobs:    blobs,
		readOnly: opts.ReadOnly,
	}

	// read header if index file is initialized, or initialize if
	// the file is empty.
	if err := idx.open(); err != nil {
		_ = idx.Close()
		return nil, err
	}

	// memory map the entire file and lock the file to prevent swap
	if err := idx.file.MMap(flag, true); err != nil {
		_ = idx.Close()
		return nil, err
	}

	return idx, nil
}

// LinearHash implements on-disk hash table using Linear Hashing
// algorithm.
type LinearHash struct {
	header
	mu       *sync.RWMutex
	file     io.File
	blobs    BlobStore
	closed   bool
	readOnly bool
}

// Get returns value associated with the given key. If the key is not
// found, returns ErrKeyNotFound.
func (idx *LinearHash) Get(key []byte) ([]byte, error) {
	hash := idx.hash(key)

	idx.mu.RLock()
	entry, err := idx.getEntry(key, hash)
	if err != nil {
		return nil, err
	}
	idx.mu.RUnlock()

	blob, err := idx.blobs.Fetch(int64(entry.BlobID))
	if err != nil {
		return nil, err
	}
	_, val := unpackKV(blob, int(entry.KeySz))
	return val, nil
}

// Put inserts/updates the key-value pair into the store.
func (idx *LinearHash) Put(key, val []byte) error {
	if idx.isImmutable() {
		return index.ErrImmutable
	}

	hash := idx.hash(key)
	blob := packKV(key, val)
	blobID, err := idx.blobs.Alloc(blob)
	if err != nil {
		return err
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.putEntry(indexEntry{
		Hash:   hash,
		KeySz:  uint64(len(key)),
		ValSz:  uint64(len(val)),
		BlobID: uint64(blobID),
		Key:    key,
	})
}

// Close flushes any pending writes and frees underlying file handle.
func (idx *LinearHash) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}
	err := idx.file.Close()
	idx.closed = true
	return err
}

func (idx *LinearHash) String() string {
	return fmt.Sprintf(
		"LinearHash{name='%s', closed=%t}",
		idx.file.Name(), idx.closed,
	)
}

func (idx *LinearHash) getEntry(key []byte, hash uint64) (*indexEntry, error) {
	return nil, nil
}

func (idx *LinearHash) putEntry(entry indexEntry) error {
	return nil
}

func (idx *LinearHash) isImmutable() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.readOnly || idx.closed
}

func (idx *LinearHash) open() error {
	size, err := idx.file.Size()
	if err != nil {
		return err
	}

	if size == 0 { // empty file, so initialize it
		return idx.init()
	}

	// open existing index file and read the header from first
	// page.
	h := header{}
	if err := io.BinaryRead(idx.file, 0, os.Getpagesize(), &h); err != nil {
		return err
	}
	if err := h.Validate(); err != nil {
		return err
	}
	idx.header = h
	return nil
}

func (idx *LinearHash) hash(key []byte) uint64 {
	hasher := maphash.Hash{}
	if _, err := hasher.Write(key); err != nil {
		panic(err) // should never return error
	}
	return hasher.Sum64()
}

func (idx *LinearHash) init() error {
	if idx.readOnly {
		return errors.New("un-initialized index file opened in read-only mode")
	}

	if err := idx.file.Truncate(int64(os.Getpagesize())); err != nil {
		return err
	}

	idx.header = header{
		magic:   kiwiMagic,
		pageSz:  uint16(os.Getpagesize()),
		version: version,
	}

	return io.BinaryWrite(idx.file, 0, idx.header)
}

// BlobStore implementations provide binary storage facilities.
type BlobStore interface {
	Alloc(blob []byte) (blobID int64, err error)
	Fetch(blobID int64) ([]byte, error)
	Free(blobID int64) error
}
