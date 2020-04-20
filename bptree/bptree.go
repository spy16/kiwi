package bptree

import (
	"errors"
	"sync"

	"github.com/spy16/kiwi/blob"
)

// New initializes an instance of B+ tree with given pager.
func New(p blob.Pager) (*BPlusTree, error) {
	tree := &BPlusTree{
		mu:      &sync.RWMutex{},
		pager:   p,
		root:    nil,
		pageSz:  p.PageSize(),
		keySize: 256,
	}

	if err := tree.open(); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree represents an on-disk B+ tree.
type BPlusTree struct {
	mu      *sync.RWMutex
	rootID  int
	root    *node
	size    int
	pager   blob.Pager
	pageSz  int
	keySize int
}

// Close flushes any writes and closes the underlying pager.
func (tree *BPlusTree) Close() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.pager == nil {
		return nil
	}

	err := tree.pager.Close()
	tree.pager = nil
	return err
}

func (tree *BPlusTree) setPageSz(pageSz int) {
	tree.pageSz = pageSz
}

func (tree *BPlusTree) open() error {
	if tree.pager.Count() == 0 {
		if err := tree.init(); err != nil {
			_ = tree.Close()
			return err
		}
	}

	d, err := tree.pager.Fetch(0)
	if err != nil {
		return err
	}

	meta := metadata{}
	if err := meta.UnmarshalBinary(d); err != nil {
		return err
	}

	if meta.version != 0x1 {
		return errors.New("incompatible version")
	}

	tree.pageSz = int(meta.pageSz)
	tree.keySize = int(meta.maxKeySz)
	tree.rootID = int(meta.root)
	tree.size = int(meta.size)
	return nil

}

func (tree *BPlusTree) init() error {
	// allocate 2 sequential pages (1 for metadata, another for root node)
	metaPage, err := tree.pager.Alloc(2)
	if err != nil {
		return err
	}

	meta := metadata{
		version:  0x1,
		flags:    0,
		size:     0,
		root:     uint32(metaPage + 1),
		pageSz:   uint16(tree.pageSz),
		maxKeySz: uint16(tree.keySize),
	}

	d, err := meta.MarshalBinary()
	if err != nil {
		return err
	}

	return tree.pager.Write(metaPage, d)
}
