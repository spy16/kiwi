package kiwi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

var (
	// ErrClosed is returned when an operation is performed on a closed
	// Kiwi DB instance.
	ErrClosed = errors.New("invalid operation on closed db")

	// ErrNotFound is returned when value for a key is not found in the
	// database.
	ErrNotFound = errors.New("key not found")

	// ErrReadOnly is returned if a put is invoked on a readonly instance.
	ErrReadOnly = errors.New("put/delete not allowed on read-only")
)

// Open opens the file as Kiwi Database file.
func Open(storage Storage, opts *Options) (*DB, error) {
	ctx, cancel := context.WithCancel(context.Background())

	db := &DB{
		isOpen:     true,
		isReadOnly: opts.ReadOnly,
		storage:    storage,
		cancelSync: cancel,
	}

	if !db.isReadOnly {
		go db.startSync(ctx, opts.Interval)
	}

	return db, nil
}

// DB represents an instance of Kiwi database. DB provides functions to
// perform any reads/writes to the DB.
type DB struct {
	isOpen     bool
	isReadOnly bool
	storage    Storage
	cancelSync func()

	syncErr error
}

// Storage implementations provide storage backend for kiwi database.
type Storage interface {
	Get(key []byte) (val []byte, err error)
	Put(key, val []byte) error
	Del(key []byte) error
}

// Syncer can be implemented by storage backends if regular file system
// syncs are needed.
type Syncer interface {
	Sync(ctx context.Context) error
}

// Get returns the value associated with the given key. Returns ErrNotFound
// if the key is not found.
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.storage.Get(key)
}

// Put puts the key-value pair into the database.
func (db *DB) Put(key, val []byte) error {
	if db.isReadOnly {
		return ErrReadOnly
	}
	return db.storage.Put(key, val)
}

// Del removes the entry with the given key from the database.
func (db *DB) Del(key []byte) error {
	if db.isReadOnly {
		return ErrReadOnly
	}
	return db.storage.Del(key)
}

func (db *DB) startSync(ctx context.Context, interval time.Duration) {
	syncer, ok := db.storage.(Syncer)
	if !ok {
		return
	}

	for {
		tick := time.NewTicker(interval)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-tick.C:
				if err := syncer.Sync(ctx); err != nil {
					db.syncErr = err
					return
				}
			}
		}
	}
}

// Close flushes all pending writes and releases locks and closes the file.
func (db *DB) Close() error {
	if !db.isOpen {
		return ErrClosed
	}
	db.isOpen = false

	return nil
}

func (db *DB) String() string {
	return fmt.Sprintf("DB{ReadOnly: %t}", db.isReadOnly)
}

// Options can be use to control how the database is opened and managed.
type Options struct {
	Mode     os.FileMode
	ReadOnly bool
	Interval time.Duration
}
