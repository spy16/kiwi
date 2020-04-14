package bptree

import (
	"os"
	"sync"

	"github.com/spy16/kiwi/io"
)

// Open opens file at given filePath as B+ tree index file and returns BPlusTree
// instance. If 'opts' value is nil, default options are used.
func Open(filePath string, opts *Options) (*BPlusTree, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	flag := os.O_CREATE | os.O_RDWR
	if opts.ReadOnly {
		flag = os.O_RDONLY
	}

	fh, err := io.OpenFile(filePath, flag, opts.FileMode)
	if err != nil {
		return nil, err
	}

	tree := &BPlusTree{
		mu:     &sync.RWMutex{},
		file:   fh,
		order:  opts.Order,
		pageSz: os.Getpagesize(),
	}

	if err := tree.open(); err != nil {
		_ = fh.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree implements an on-disk B+ Tree.
type BPlusTree struct {
	order  int
	pageSz int

	mu   *sync.RWMutex
	file io.File
}

// Put inserts the key value pair into the tree.
func (tree *BPlusTree) Put(key, val []byte) error {
	return nil
}

// Close flushes any pending writes and frees the file descriptor.
func (tree *BPlusTree) Close() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.file == nil {
		return nil
	}

	err := tree.file.Close()
	tree.file = nil
	return err
}

func (tree *BPlusTree) open() error {
	size, err := tree.file.Size()
	if err != nil {
		return err
	}

	if size == 0 { // initialize new B+ tree file.
		return tree.init()
	}

	h := header{}
	if err := io.BinaryRead(tree.file, 0, headerSz, &h); err != nil {
		return err
	} else if h.Validate(); err != nil {
		return err
	}

	tree.pageSz = int(h.pageSz)
	tree.order = int(h.order)
	return nil
}

func (tree *BPlusTree) init() error {
	h := &header{
		magic:   0xABCDEF,
		version: 0x1,
		flags:   0,
		pageSz:  uint16(tree.pageSz),
		order:   uint16(tree.order),
	}

	if err := tree.file.Truncate(int64(tree.pageSz)); err != nil {
		return err
	}

	return io.BinaryWrite(tree.file, 0, h)
}
