package kiwi

import (
	"errors"
	"fmt"
	"os"
)

// DefaultOptions provides some sane defaults to DB options and is
// used when nil value is passed to Open.
var DefaultOptions = &Options{
	Mode:     os.ModePerm,
	ReadOnly: false,
}

var (
	// ErrClosed is returned when an operation is performed on a closed
	// Kiwi DB instance.
	ErrClosed = errors.New("invalid operation on closed db")
)

// Open opens the file as Kiwi Database file. If the file does not exists,
// it will be created. If the file is empty, it will be initialized with
// Kiwi database format.
func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions
	}

	flag := os.O_RDWR
	if opts.ReadOnly {
		flag = os.O_RDONLY
	}

	fh, err := os.OpenFile(path, flag|os.O_CREATE, opts.Mode)
	if err != nil {
		return nil, err
	}

	db := &DB{
		isOpen:     true,
		isReadOnly: opts.ReadOnly,
		fh:         fh,
		path:       path,
	}

	// TODO: Should get an exclusive file lock if not readonly.

	return db, nil
}

// DB represents an instance of Kiwi database. DB provides functions to
// perform any reads/writes to the DB.
type DB struct {
	isOpen     bool
	isReadOnly bool
	path       string
	fh         *os.File
}

// Close flushes all pending writes and releases locks and closes the file.
func (db *DB) Close() error {
	if !db.isOpen {
		return ErrClosed
	}
	db.isOpen = false

	if db.fh != nil {
		if err := db.fh.Close(); err != nil {
			return err
		}
		db.fh = nil
	}

	db.path = ""
	return nil
}

func (db *DB) String() string {
	return fmt.Sprintf("DB{File=%s, ReadOnly: %t}", db.fh.Name(), db.isReadOnly)
}

// Options can be use to control how the database is opened and managed.
type Options struct {
	Mode     os.FileMode
	ReadOnly bool
}
