package bptree

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/spy16/kiwi/io"
)

const version = uint8(0x1)

// Open opens the named file as a B+ tree index file.
func Open(fileName string, readOnly bool, mode os.FileMode) (*BPlusTree, error) {
	p, err := io.Open(fileName, readOnly, mode)
	if err != nil {
		return nil, err
	}

	tree := &BPlusTree{
		mu:      &sync.RWMutex{},
		pager:   p,
		root:    nil,
		keySize: 256,
	}
	tree.setPageSz(os.Getpagesize())

	if err := tree.open(); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree represents an on-disk B+ tree.
type BPlusTree struct {
	keySize    int
	degree     int
	maxEntries int

	// tree states
	mu     *sync.RWMutex
	root   *node
	size   int
	pager  *io.Pager
	rootID int
	pageSz int
	nodes  map[int]*node
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

func (tree *BPlusTree) String() string {
	return fmt.Sprintf(
		"BPlusTree{pager=%v}",
		tree.pager,
	)
}

func (tree *BPlusTree) isOverflow(n *node) bool {
	if n.IsLeaf() {
		return len(n.entries) > tree.maxEntries
	}
	return len(n.children) > tree.degree
}

func (tree *BPlusTree) leafKey(n *node) ([]byte, error) {
	if n.IsLeaf() {
		return n.entries[0].key, nil
	}

	child, err := tree.fetch(n.children[0])
	if err != nil {
		return nil, err
	}

	return tree.leafKey(child)
}

func (tree *BPlusTree) alloc() (*node, error) {
	return nil, nil
}

func (tree *BPlusTree) fetch(id int) (*node, error) {
	return nil, nil
}

func (tree *BPlusTree) writeAll() error {
	return nil
}

func (tree *BPlusTree) setPageSz(pageSz int) {
	tree.pageSz = pageSz
	// TODO: calculate internal & leaf node degrees based on keySz & pageSz
}

func (tree *BPlusTree) open() error {
	if tree.pager.Count() == 0 {
		if err := tree.init(); err != nil {
			_ = tree.Close()
			return err
		}

		return nil
	}

	d, err := tree.pager.Read(0)
	if err != nil {
		return err
	}

	meta := metadata{}
	if err := meta.UnmarshalBinary(d); err != nil {
		return err
	}

	if meta.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", meta.version, version)
	} else if tree.pager.PageSize() != int(meta.pageSz) {
		return errors.New("page size in meta does not match pager")
	}

	tree.pageSz = int(meta.pageSz)
	tree.keySize = int(meta.maxKeySz)
	tree.rootID = int(meta.root)
	tree.size = int(meta.size)
	return nil

}

func (tree *BPlusTree) init() error {
	// allocate 2 sequential pages (1 for metadata, another for root node)
	metaPage, err := tree.pager.Alloc(1)
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
