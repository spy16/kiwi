package index

import "errors"

var (
	// ErrNotFound should be returned by backends when a key is not found
	// in the storage.
	ErrNotFound = errors.New("key not found")

	// ErrEmptyKey should be returned by backends when an operation is
	// requested with an empty key.
	ErrEmptyKey = errors.New("invalid sized key")

	// ErrOpNotAllowed should be returned by backends when write operation
	// (put/del) is attempted on a readonly.
	ErrOpNotAllowed = errors.New("operation not allowed in read-only mode")
)
