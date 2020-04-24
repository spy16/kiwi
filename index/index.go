package index

import (
	"errors"
)

var (
	// ErrKeyNotFound should be returned from lookup operations when the
	// lookup key is not found in index/store.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyTooLarge is returned by index implementations when a key is
	// larger than a configured limit if any.
	ErrKeyTooLarge = errors.New("key is too large")

	// ErrEmptyKey should be returned by backends when an operation is
	// requested with an empty key.
	ErrEmptyKey = errors.New("empty key")

	// ErrImmutable should be returned by backends when write operation
	// (put/del) is attempted on a readonly.
	ErrImmutable = errors.New("operation not allowed in read-only mode")
)

// Index represents the indexing scheme to be used by Kiwi database
// instance.
type Index interface {
	Get(key []byte) (uint64, error)
	Del(key []byte) (uint64, error)
	Put(key []byte, v uint64) error
}

// Scanner represents indexing schemes with support for prefix
// scans.
type Scanner interface {
	Index
	Scan(beginKey []byte, reverse bool, scanFn func(key []byte, v uint64) bool) error
}
