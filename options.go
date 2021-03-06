package kiwi

import "os"

// Indexing schemes supported.
const (
	BPlusTree IndexType = 0
)

// DefaultOptions provides some sane defaults for initializing Kiwi DB.
var DefaultOptions = Options{
	IndexType: BPlusTree,
	ReadOnly:  false,
	FileMode:  0664,
	Log:       func(msg string, args ...interface{}) {},
}

// Options represents configuration settings for kiwi database.
type Options struct {
	IndexType IndexType
	ReadOnly  bool
	FileMode  os.FileMode
	Log       func(msg string, args ...interface{})
}

// IndexType represents the type of the index to be used by Kiwi.
type IndexType int
