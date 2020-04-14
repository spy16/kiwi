package bptree

import "os"

var defaultOptions = Options{
	ReadOnly: false,
	FileMode: os.ModePerm,
	Order:    5,
}

// Options represents configuration options for the B+ tree.
type Options struct {
	ReadOnly bool
	FileMode os.FileMode
	Order    int
}
