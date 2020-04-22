package kiwi

import "os"

// Indexing shemes supported.
const (
	BPlusTree IndexType = 0
)

var defaultOptions = Options{
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
