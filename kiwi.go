package kiwi

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

const (
	kiwiMagic = uint32(0x6B697769)
	dbVersion = uint32(0x1)
)

var (
	// ErrNotFound is returned when value for a key is not found in the
	// database.
	ErrNotFound = errors.New("key not found")

	// ErrImmutable is returned when a mutating operation is requested on
	// a closed or read-only database instance.
	ErrImmutable = errors.New("can't put/delete into closed/read-only DB")
)

// Open opens the kiwi database file at given filePath. If the filePath is ":memory:"
// an in-memory instance (without any persistence) is created.
func Open(filePath string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &defaultOptions
	} else if opts.Log == nil {
		opts.Log = func(msg string, args ...interface{}) {}
	}

	db := &DB{
		// populate configs
		filePath:   filePath,
		isReadOnly: opts.ReadOnly,

		// internal states
		mu:     &sync.RWMutex{},
		isOpen: false,
	}

	if err := db.open(opts.FileMode); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.isOpen = true

	return db, nil
}

// DB represents an instance of Kiwi database.
type DB struct {
	// external configs
	filePath   string
	isReadOnly bool

	// internal state
	mu      *sync.RWMutex
	isOpen  bool
	backend Backend
}

// Get returns the value associated with the given key. Returns ErrNotFound
// if the key is not found.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.backend.Get(key)
}

// Put puts the given key-value pair into the kiwi database. If the key is
// already in the db, value is updated. Returns error if the db is read-only
// or is closed.
func (db *DB) Put(key, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.isMutable() {
		return ErrImmutable
	}

	return db.backend.Put(key, val)
}

// Del removes the entry with the given key from the kiwi store. Returns
// errors if the db is read-only or is closed.
func (db *DB) Del(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.isMutable() {
		return ErrImmutable
	}

	return db.backend.Del(key)
}

// Close closes the underlying files and the indexers.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.isOpen {
		return nil
	}

	err := db.backend.Close()
	db.isOpen = false
	return err
}

func (db *DB) open(mode os.FileMode) error {
	if db.filePath == ":memory:" {
		db.backend = &inMemory{}
		return nil
	}

	return fmt.Errorf("failed to detect backend from file '%s'", db.filePath)
}

func (db *DB) String() string {
	return fmt.Sprintf("DB{file='%s', readOnly=%t}", db.filePath, db.isReadOnly)
}

func (db *DB) isMutable() bool { return db.isReadOnly || !db.isOpen }
