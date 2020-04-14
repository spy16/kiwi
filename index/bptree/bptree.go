package bptree

import (
	"fmt"
	"os"
	"sync"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/io"
)

const (
	// magic marker to indicate B+ tree index file.
	// hex version of 'bptree'
	magic   = uint32(0x62707472)
	version = 0x1
)

var _ index.Index = (*BPlusTree)(nil)

// Open opens file at filePath as B+ tree index file and returns BPlusTree
// instance. If 'opts' value is nil, defaultOptions will be used.
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
		mu:       &sync.RWMutex{},
		file:     fh,
		pageSz:   os.Getpagesize(),
		readOnly: opts.ReadOnly,
	}

	if err := tree.open(); err != nil {
		_ = fh.Close()
		return nil, err
	}

	if err := tree.file.MMap(io.RDWR, true); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree implements an on-disk B+ Tree.
type BPlusTree struct {
	mu       *sync.RWMutex
	file     io.File
	pageSz   int
	readOnly bool
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

func (tree *BPlusTree) String() string {
	return fmt.Sprintf(
		"BPlusTree{name='%s', closed=%t}",
		tree.file.Name(), tree.file == nil,
	)
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
	return nil
}

func (tree *BPlusTree) init() error {
	if tree.isImmutable() {
		return index.ErrImmutable
	}

	h := &header{
		magic:   magic,
		version: version,
		pageSz:  uint16(tree.pageSz),
	}

	if err := tree.file.Truncate(int64(tree.pageSz)); err != nil {
		return err
	}

	return io.BinaryWrite(tree.file, 0, h)
}

func (tree *BPlusTree) isImmutable() bool {
	return tree.readOnly || tree.file == nil
}
