package kiwi

import (
	"fmt"
	"os"
	"sync"

	"github.com/spy16/kiwi/io"
)

// Open opens the named file as Kiwi database and returns a DB instance for
// accessing it. If the file doesn't exist, it will be created and initialzed
// if not in read-only mode.
func Open(filePath string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &DefaultOptions
	}

	if opts.Log == nil {
		opts.Log = func(msg string, args ...interface{}) {}
	}

	bf, err := io.Open(filePath, os.Getpagesize(), opts.ReadOnly, opts.FileMode)
	if err != nil {
		return nil, err
	}

	return &DB{
		mu:         &sync.RWMutex{},
		file:       bf,
		isOpen:     true,
		filePath:   filePath,
		isReadOnly: opts.ReadOnly,
	}, nil
}

// DB represents an instance of Kiwi database.
type DB struct {
	// external configs
	filePath   string
	isReadOnly bool

	// internal state
	mu     *sync.RWMutex
	file   io.BlockFile
	isOpen bool
}

// Close closes the underlying files and the indexers.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.mu.RLock()

	if !db.isOpen {
		return nil
	}

	err := db.file.Close()
	db.isOpen = false
	return err
}

func (db *DB) String() string {
	return fmt.Sprintf("DB{file='%s', readOnly=%t}", db.filePath, db.isReadOnly)
}

func (db *DB) isMutable() bool { return db.isReadOnly || !db.isOpen }
