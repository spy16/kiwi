package bptree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/spy16/kiwi/index"
)

const version = uint8(0x1)

var (
	// bin is the byte order used by all marshal/unmarshal operations.
	bin = binary.LittleEndian

	// defaultOptions to be used by New().
	defaultOptions = Options{
		MaxKeySize: 100,
	}
)

// New initializes a new instance of B+ tree using the given pager.
func New(p Pager, opts *Options) (*BPlusTree, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	tree := &BPlusTree{
		mu:    &sync.RWMutex{},
		pager: p,
		root:  nil,
		nodes: map[int]*node{},
		log:   log.Printf,
	}

	if err := tree.open(*opts); err != nil {
		_ = tree.Close()
		return nil, err
	}

	if err := tree.computeDegree(p.PageSize()); err != nil {
		_ = tree.Close()
		return nil, err
	}

	if err := tree.fetchRoot(); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree represents an on-disk B+ tree. Each node in the tree is mapped
// to a single page in the file. Degree of the tree is decided based on the
// page size and max key size while initializing.
type BPlusTree struct {
	meta           metadata
	leafDegree     int // max number of keys per leaf node
	internalDegree int // max number of keys per internal node

	// tree states
	mu    *sync.RWMutex
	pager Pager         // paged file
	nodes map[int]*node // node cache
	root  *node         // root node
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
	if len(key) > int(tree.meta.maxKeySz) {
		return errors.New("key is too large")
	} else if len(key) == 0 {
		return errors.New("empty key")
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	if err := tree.canMutate(); err != nil {
		return err
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
		tree.meta.size++
		tree.meta.dirty = true
	}

	// write all the nodes that were modified/created
	return tree.writeAll()
}

// Del removes the key from the B+ tree and returns that existed for the key.
func (tree *BPlusTree) Del(key []byte) (uint64, error) {
	if len(key) > int(tree.meta.maxKeySz) {
		return 0, errors.New("key is too large")
	} else if len(key) == 0 {
		return 0, index.ErrEmptyKey
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	if err := tree.canMutate(); err != nil {
		return 0, err
	}

	leaf, idx, found, err := tree.searchRec(tree.root, key)
	if err != nil {
		return 0, err
	} else if !found {
		return 0, index.ErrKeyNotFound
	}

	// TODO: delete the key from the leaf node and rebalance if required

	return leaf.entries[idx].val, errors.New("not implemented")
}

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFn. If the key is zero valued (nil or len=0), then the
// left most leaf key will be used as the starting key.
// Scan continues until the right most leaf node is reached or the scanFn
// returns 'true' indicating to stop the scan.
// TODO: all nodes are cached in-memory during scan which might not be good.
func (tree *BPlusTree) Scan(key []byte, scanFn func(key []byte, v uint64) bool) error {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if tree.meta.size == 0 {
		return nil
	}

	var err error
	if len(key) == 0 {
		// No explicit key provided by user, find the left-most leaf-key.
		key, err = tree.leafKey(tree.root)
		if err != nil {
			return err
		}
	}

	// find the leaf node with the given key or the leaf node that would
	// contain the given key if it existed.
	leaf, _, _, err := tree.searchRec(tree.root, key)
	if err != nil {
		return err
	}

	// starting at found leaf node, follow the 'next' pointer until.
	for leaf != nil {
		for i := 0; i < len(leaf.entries); i++ {
			e := leaf.entries[i]
			if scanFn(e.key, e.val) {
				break
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
func (tree *BPlusTree) Size() int64 { return int64(tree.meta.size) }

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
	return fmt.Sprintf("BPlusTree{pager=%v, size=%d}", tree.pager, tree.meta.size)
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

	if !isInsert {
		return false, nil
	}

	if tree.isOverflow(child) {
		nodes, err := tree.alloc(1)
		if err != nil {
			return false, err
		}
		right := nodes[0]

		child.SplitRight(right)

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

	// we need a new root and a new right node. so allocate
	// 2 at once.
	nodes, err := tree.alloc(2)
	if err != nil {
		return err
	}

	oldRoot := tree.root
	newRoot := nodes[0]
	rightSibling := nodes[1]

	oldRoot.SplitRight(rightSibling)

	leafKey, err := tree.leafKey(rightSibling)
	if err != nil {
		return err
	}

	newRoot.entries = []entry{{key: leafKey}}
	newRoot.children = []int{oldRoot.id, rightSibling.id}

	tree.root = newRoot
	tree.nodes[newRoot.id] = newRoot
	tree.meta.rootID = uint32(newRoot.id)
	tree.meta.dirty = true
	return nil
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
		n := newNode(firstID+i, int(tree.meta.pageSz))
		tree.nodes[n.id] = n
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// fetchRoot reads the root from the underlying pager and sets it on the tree
// instance if not already set.
func (tree *BPlusTree) fetchRoot() error {
	if tree.root != nil {
		return nil
	}

	r, err := tree.fetch(int(tree.meta.rootID))
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
	n = newNode(id, int(tree.meta.pageSz))
	if err := tree.pager.Unmarshal(id, n); err != nil {
		return nil, err
	}
	n.dirty = false // we just read this node, not dirty
	tree.nodes[n.id] = n
	return n, nil
}

// writeAll writes all the nodes marked dirty to the underlying pager.
func (tree *BPlusTree) writeAll() error {
	if tree.pager.ReadOnly() {
		return nil
	}

	for _, n := range tree.nodes {
		if n.dirty {
			if err := tree.pager.Marshal(n.id, n); err != nil {
				return err
			}
			n.dirty = false
		}
	}

	return tree.writeMeta()
}

// computeDegree computes the degree of the tree based on page-size and the
// maximum key size.
func (tree *BPlusTree) computeDegree(pageSz int) error {
	// available for node content in leaf/internal nodes
	leafContentSz := (pageSz - leafNodeHeaderSz)
	internalContentSz := (pageSz - internalNodeHeaderSz)

	const valueSz = 8       // for the uint64 value
	const childPtrSz = 4    // for uint32 child pointer in non-leaf node
	const keySizeSpecSz = 2 // for storing the actual key size

	leafEntrySize := int(valueSz + 2 + tree.meta.maxKeySz)                    // 8 bytes for the uint64 value
	internalEntrySize := int(childPtrSz + keySizeSpecSz + tree.meta.maxKeySz) // 4 bytes for the uint32 child pointer

	tree.leafDegree = leafContentSz / leafEntrySize

	// 4 bytes extra for the one extra child pointer
	tree.internalDegree = (internalContentSz - 4) / internalEntrySize

	if tree.leafDegree <= 2 || tree.internalDegree <= 2 {
		return errors.New("invalid degree, reduce key size or increase page size")
	}
	return nil
}

func (tree *BPlusTree) open(opts Options) error {
	if tree.pager.Count() == 0 {
		return tree.init(opts)
	}

	if err := tree.pager.Unmarshal(0, &tree.meta); err != nil {
		return err
	}

	if tree.meta.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", tree.meta.version, version)
	} else if tree.pager.PageSize() != int(tree.meta.pageSz) {
		return errors.New("page size in meta does not match pager")
	}

	return nil

}

func (tree *BPlusTree) init(opts Options) error {
	// allocate 2 pages (1 for meta + 1 for root)
	_, err := tree.pager.Alloc(2)
	if err != nil {
		return err
	}

	tree.root = newNode(1, tree.pager.PageSize())
	tree.nodes[tree.root.id] = tree.root

	tree.meta = metadata{
		dirty:    true,
		version:  version,
		flags:    0,
		size:     0,
		rootID:   1,
		pageSz:   uint16(tree.pager.PageSize()),
		maxKeySz: uint16(opts.MaxKeySize),
	}
	return nil
}

func (tree *BPlusTree) writeMeta() error {
	if tree.meta.dirty {
		err := tree.pager.Marshal(0, tree.meta)
		tree.meta.dirty = false
		return err
	}
	return nil
}

func (tree *BPlusTree) canMutate() error {
	if tree.pager == nil {
		return os.ErrClosed
	} else if tree.pager.ReadOnly() {
		return index.ErrImmutable
	}
	return nil
}

// Options represents the configuration options for the B+ tree index.
type Options struct {
	MaxKeySize int
}
