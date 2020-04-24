package kiwi

import (
	"fmt"
	"io"
	"sync"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/index/bptree"
)

var _ Index = (*bptree.BPlusTree)(nil)
var _ IndexScanner = (*bptree.BPlusTree)(nil)

// Open opens a Kiwi database.
func Open(filePath string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	if opts.Log == nil {
		opts.Log = func(msg string, args ...interface{}) {}
	}

	return nil, nil
}

// Index represents the indexing scheme to be used by Kiwi database
// instance.
type Index interface {
	Get(key []byte) (uint64, error)
	Del(key []byte) (uint64, error)
	Put(key []byte, v uint64) error
}

// IndexScanner represents indexing schemes with support for prefix
// scans.
type IndexScanner interface {
	Index
	Scan(beginKey []byte, reverse bool, scanFn func(key []byte, v uint64) bool) error
}

// BlobStore represents a storage for arbitrary blobs of binary data.
type BlobStore interface {
	io.Closer

	// Write should write the blob to the storage. If the value of id
	// is -1, a new record should be allocated and the blob id should
	// be returned. Implementation is free to allocate new blob even
	// if the id is not -1.
	Write(id int, d []byte) (uint64, error)

	// Fetch should return the blob with given identifier.
	Fetch(id uint64) ([]byte, error)

	// Free should delete the blob with given identifier. Same id can
	// be re-used for newer blobs.
	Free(id uint64) error
}

// DB represents an instance of Kiwi database.
type DB struct {
	// external configs
	filePath   string
	isReadOnly bool

	// internal state
	mu     *sync.RWMutex
	index  Index
	blobs  BlobStore
	isOpen bool
}

// Get returns the value associated with the given key. Returns ErrNotFound
// if the key is not found.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, err := db.index.Get(key)
	if err != nil {
		return nil, err
	}

	d, err := db.blobs.Fetch(offset)
	if err != nil {
		return nil, err
	}

	return d, nil
}

// Put puts the given key-value pair into the kiwi database. If the key is
// already in the db, value is updated. Returns error if the db is read-only
// or is closed.
func (db *DB) Put(key, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.isMutable() {
		return index.ErrImmutable
	}

	blob := db.makeBlob(key, val)

	offset, err := db.blobs.Write(-1, blob)
	if err != nil {
		return err
	}

	err = db.index.Put(key, offset)
	if err != nil {
		_ = db.blobs.Free(offset) // rollback
		return err
	}

	return nil
}

// Del removes the entry with the given key from the kiwi store. Returns
// errors if the db is read-only or is closed.
func (db *DB) Del(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.isMutable() {
		return index.ErrImmutable
	}

	off, err := db.index.Del(key)
	if err != nil {
		return err
	}

	return db.blobs.Free(off)
}

// Close closes the underlying files and the indexers.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.isOpen {
		return nil
	}

	if closer, ok := db.index.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	err := db.blobs.Close()
	db.isOpen = false
	return err
}

func (db *DB) String() string {
	return fmt.Sprintf("DB{file='%s', readOnly=%t}", db.filePath, db.isReadOnly)
}

func (db *DB) isMutable() bool { return db.isReadOnly || !db.isOpen }

func (db *DB) makeBlob(k, v []byte) []byte {
	d := make([]byte, len(k)+len(v))
	// TODO: add checksum
	copy(d[:len(k)], k)
	copy(d[len(k):], v)
	return d
}
