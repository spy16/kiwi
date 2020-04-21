package bptree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/io"
)

const (
	maxKeySz = 4
	version  = uint8(0x1)
)

var bin = binary.LittleEndian

// Open opens the named file as a B+ tree index file. If the file does not
// exist, it will be created if not in read-only mode.
func Open(fileName string, readOnly bool, mode os.FileMode) (*BPlusTree, error) {
	p, err := io.Open(fileName, readOnly, mode)
	if err != nil {
		return nil, err
	}

	tree := &BPlusTree{
		mu:    &sync.RWMutex{},
		pager: p,
		root:  nil,
		nodes: map[int]*node{},
		log:   log.Printf,
	}

	if err := tree.open(); err != nil {
		_ = tree.Close()
		return nil, err
	}
	tree.computeDegree(p.PageSize())

	if err := tree.fetchRoot(); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree represents an on-disk B+ tree. Each node in the tree is mapped
// to a single page in the file. Order of the tree is  decided based on the
// page size while initializing.
type BPlusTree struct {
	metadata
	leafDegree     int // max number of keys per leaf node
	internalDegree int // max number of keys per internal node

	// tree states
	mu    *sync.RWMutex
	pager *io.Pager
	nodes map[int]*node
	root  *node
	log   func(msg string, args ...interface{})
}

// Get finds and returns the value associated with the given key. If the key
// is not found, returns ErrNotFound.
func (tree *BPlusTree) Get(key []byte) (uint64, error) {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if len(tree.root.entries) == 0 {
		return 0, errors.New("no entries")
	}

	n, idx, found, err := tree.searchRec(tree.root, key)
	if err != nil {
		return 0, err
	}

	if !found {
		return 0, index.ErrKeyNotFound
	}

	return n.entries[idx].val, nil
}

// Put puts the key-value pair into the B+ tree. If the key already exists, its
// value will be updated.
func (tree *BPlusTree) Put(key []byte, val uint64) error {
	if len(key) > int(tree.maxKeySz) {
		return errors.New("key is too large")
	} else if len(key) == 0 {
		return errors.New("empty key")
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.pager == nil {
		return os.ErrClosed
	} else if tree.pager.ReadOnly() {
		return index.ErrImmutable
	}

	e := entry{
		key: append([]byte(nil), key...),
		val: val,
	}

	isInsert, err := tree.nodePut(tree.root, e)
	if err != nil {
		return err
	}

	if isInsert {
		tree.size++
	}

	// write all the nodes that were modified/created
	return tree.writeAll()
}

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFunc.
func (tree *BPlusTree) Scan(key []byte, scanFunc func(key []byte, v uint64) error) error {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	var err error
	if len(key) == 0 {
		key, err = tree.leafKey(tree.root)
		if err != nil {
			return err
		}
	}

	leaf, _, _, err := tree.searchRec(tree.root, key)
	if err != nil {
		return err
	}

	for leaf != nil {
		for i := 0; i < len(leaf.entries); i++ {
			if err := scanFunc(leaf.entries[i].key, leaf.entries[i].val); err != nil {
				return err
			}
		}

		if leaf.next == 0 {
			break
		}

		leaf, err = tree.fetch(leaf.next)
		if err != nil {
			return err
		}
	}

	return nil
}

// Size returns the number of entries in the entire tree
func (tree *BPlusTree) Size() int64 { return int64(tree.size) }

// Close flushes any writes and closes the underlying pager.
func (tree *BPlusTree) Close() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.pager == nil {
		return nil
	}

	_ = tree.writeAll() // write if any nodes are pending
	err := tree.pager.Close()
	tree.pager = nil
	return err
}

func (tree *BPlusTree) String() string {
	return fmt.Sprintf(
		"BPlusTree{pager=%v, size=%d, leafDegree=%d, internalDegree=%d}",
		tree.pager, tree.size, tree.leafDegree, tree.internalDegree,
	)
}

// nodePut recursively traverses the sub-tree with given root node until
// leaf node is reached and inserts the entry into it.
func (tree *BPlusTree) nodePut(n *node, e entry) (bool, error) {
	idx, found := n.Search(e.key)

	if n.IsLeaf() {
		if found {
			n.SetVal(idx, e.val)
			return false, nil
		}

		n.InsertAt(idx, e)
		return true, tree.splitRootIfNeeded()
	}

	if found {
		idx++
	}

	child, err := tree.fetch(n.children[idx])
	if err != nil {
		return false, err
	}

	isInsert, err := tree.nodePut(child, e)
	if err != nil {
		return false, err
	}

	if tree.isOverflow(child) {
		right, err := tree.split(child)
		if err != nil {
			return false, err
		}

		leafKey, err := tree.leafKey(right)
		if err != nil {
			return false, err
		}
		n.AddChild(leafKey, right)
	}

	return isInsert, tree.splitRootIfNeeded()
}

// splitRootIfNeeded splits the root node if it is overflowing and creates
// a new root for the tree. 2 allocs will be done (1 for root node, another
// for the new right sibling of the old root)
func (tree *BPlusTree) splitRootIfNeeded() error {
	if !tree.isOverflow(tree.root) {
		// splitting is not needed
		return nil
	}

	oldRoot := tree.root
	sibling, err := tree.split(oldRoot)
	if err != nil {
		return err
	}

	leafKey, err := tree.leafKey(sibling)
	if err != nil {
		return err
	}

	id, err := tree.pager.Alloc(1)
	if err != nil {
		return err
	}

	newRoot := newNode(id, int(tree.pageSz))
	newRoot.entries = []entry{{key: leafKey}}
	newRoot.children = []int{tree.root.id, sibling.id}

	tree.root = newRoot
	tree.rootID = uint32(id)
	tree.nodes[newRoot.id] = newRoot
	return nil
}

// split splits the given node and returns its right sibling.
func (tree *BPlusTree) split(n *node) (right *node, err error) {
	nodes, err := tree.alloc(1)
	if err != nil {
		return
	}

	right = nodes[0]
	n.SplitRight(right)
	return right, nil
}

// isOverflow returns true if the node contains more entries/children
// than allowed by the degree of the tree.
func (tree *BPlusTree) isOverflow(n *node) bool {
	if n.IsLeaf() {
		return len(n.entries) > tree.leafDegree-1
	}
	return len(n.children) > tree.internalDegree
}

// searchRec searches the sub-tree with root 'n' recursively until the key
// is  found or the leaf node is  reached. Returns the node last searched,
// index where the key should be and a flag to indicate if the key exists.
func (tree *BPlusTree) searchRec(n *node, key []byte) (*node, int, bool, error) {
	idx, found := n.Search(key)

	if n.IsLeaf() {
		return n, idx, found, nil
	}

	if found {
		idx++
	}

	child, err := tree.fetch(n.children[idx])
	if err != nil {
		return nil, 0, false, err
	}

	return tree.searchRec(child, key)
}

// leafKey traverses the sub-tree with given node as root until it reaches
// the left-most leaf node and returns its left-most key.
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

// alloc allocates a page for a new node and returns it. underlying file may
// be resized, but  no read is done in  this call and the node returned will
// be zero-valued.
func (tree *BPlusTree) alloc(n int) ([]*node, error) {
	firstID, err := tree.pager.Alloc(n)
	if err != nil {
		return nil, err
	}

	var nodes []*node
	for i := 0; i < n; i++ {
		n := newNode(firstID+i, int(tree.pageSz))
		tree.nodes[n.id] = n
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func (tree *BPlusTree) fetchRoot() error {
	if tree.root != nil {
		return nil
	}

	r, err := tree.fetch(int(tree.rootID))
	if err != nil {
		return err
	}
	tree.root = r
	return nil
}

// fetch returns the node with given id. Lookup is first done in the in-memory
// node map, if not found, page corresponding to the node will be read from file
// and cached.
func (tree *BPlusTree) fetch(id int) (*node, error) {
	n, found := tree.nodes[id]
	if found {
		return n, nil
	}
	n = newNode(id, int(tree.pageSz))
	if err := tree.pager.Unmarshal(id, n); err != nil {
		return nil, err
	}
	tree.nodes[n.id] = n
	return n, nil
}

// writeAll writes all the nodes marked dirty to the underlying pager.
func (tree *BPlusTree) writeAll() error {
	for id, n := range tree.nodes {
		if !n.dirty {
			continue
		}

		if err := tree.pager.Marshal(id, n); err != nil {
			return err
		}
	}

	return tree.writeMeta()
}

// computeDegree computes the degree of the tree based on page-size and the
// maximum key size.
func (tree *BPlusTree) computeDegree(pageSz int) {
	// available for node content in leaf/internal nodes
	leafContentSz := (pageSz - leafNodeHeaderSz)
	internalContentSz := (pageSz - internalNodeHeaderSz)

	const valueSz = 8       // for the uint64 value
	const childPtrSz = 4    // for uint32 child pointer in non-leaf node
	const keySizeSpecSz = 2 // for storing the actual key size

	leafEntrySize := int(valueSz + 2 + tree.maxKeySz)                    // 8 bytes for the uint64 value
	internalEntrySize := int(childPtrSz + keySizeSpecSz + tree.maxKeySz) // 4 bytes for the uint32 child pointer

	tree.leafDegree = leafContentSz / leafEntrySize

	// 4 bytes extra for the one extra child pointer
	tree.internalDegree = (internalContentSz - 4) / internalEntrySize
}

func (tree *BPlusTree) open() error {
	if tree.pager.Count() == 0 {
		return tree.init()
	}

	if err := tree.pager.Unmarshal(0, &tree.metadata); err != nil {
		return err
	}

	if tree.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", tree.version, version)
	} else if tree.pager.PageSize() != int(tree.pageSz) {
		return errors.New("page size in meta does not match pager")
	}

	return nil

}

func (tree *BPlusTree) init() error {
	// allocate 2 pages (1 for meta + 1 for root)
	_, err := tree.pager.Alloc(2)
	if err != nil {
		return err
	}

	tree.metadata = metadata{
		version:  version,
		flags:    0,
		size:     0,
		rootID:   1,
		pageSz:   uint16(tree.pager.PageSize()),
		maxKeySz: maxKeySz,
	}

	return tree.writeMeta()
}

func (tree *BPlusTree) writeMeta() error {
	return tree.pager.Marshal(0, tree.metadata)
}
