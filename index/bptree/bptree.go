// Package bptree implements an on-disk B+ tree indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package bptree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/pager"
)

// bin is the byte order used for all marshals/unmarshals.
var bin = binary.LittleEndian

// Open opens the named file as a B+ tree index file and returns an instance
// B+ tree for use. Use ":memory:" for an in-memory B+ tree instance for quick
// testing setup. Degree of the tree is computed based on maxKeySize and pageSize
// used by the pager. If nil options are provided, defaultOptions will be used.
func Open(fileName string, opts *Options) (*BPlusTree, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	p, err := pager.Open(fileName, opts.PageSize, opts.ReadOnly, opts.FileMode)
	if err != nil {
		return nil, err
	}

	tree := &BPlusTree{
		mu:    &sync.RWMutex{},
		file:  fileName,
		pager: p,
		root:  nil,
		nodes: map[int]*node{},
	}

	// initialize the tree if new or open the existing tree and load
	// root node.
	if err := tree.open(*opts); err != nil {
		_ = tree.Close()
		return nil, err
	}

	// compute B+ tree degree based on maxKeySize and page size.
	if err := tree.computeDegree(int(tree.meta.pageSz)); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree represents an on-disk B+ tree. Each node in the tree is mapped
// to a single page in the file. Degree of the tree is decided based on the
// page size and max key size while initializing.
type BPlusTree struct {
	file       string
	degree     int
	leafDegree int

	// tree state
	mu    *sync.RWMutex
	pager *pager.Pager
	nodes map[int]*node // node cache to avoid IO
	meta  metadata      // metadata about tree structure
	root  *node         // current root node
}

// Get fetches the value associated with the given key. Returns error if key
// not found.
func (tree *BPlusTree) Get(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, index.ErrEmptyKey
	}

	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if len(tree.root.entries) == 0 {
		return 0, index.ErrKeyNotFound
	}

	n, idx, found, err := tree.searchRec(tree.root, key)
	if err != nil {
		return 0, err
	} else if !found {
		return 0, index.ErrKeyNotFound
	}

	return n.entries[idx].val, nil
}

// Put puts the key-value pair into the B+ tree. If the key already exists,
// its value will be updated.
func (tree *BPlusTree) Put(key []byte, val uint64) error {
	if len(key) > int(tree.meta.maxKeySz) {
		return index.ErrKeyTooLarge
	} else if len(key) == 0 {
		return index.ErrEmptyKey
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	e := entry{
		key: append([]byte(nil), key...),
		val: val,
	}

	isInsert, err := tree.put(e)
	if err != nil {
		return err
	}

	if isInsert {
		tree.meta.size++
		tree.meta.dirty = true
	}

	return tree.writeAll()
}

// Del removes the key-value entry from the B+ tree. If the key does not
// exist, returns error.
func (tree *BPlusTree) Del(key []byte) (uint64, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	return 0, errors.New("not implemented")
}

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFn. If the key is zero valued (nil or len=0), then the
// left/right leaf key will be used as the starting key. Scan continues until
// the right most leaf node is reached or the scanFn returns 'true' indicating
// to stop the scan. If reverse=true, scan starts at the right most node and
// executes in descending order of keys.
func (tree *BPlusTree) Scan(key []byte, reverse bool, scanFn func(key []byte, v uint64) bool) error {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if tree.meta.size == 0 {
		return nil
	}

	var err error
	var beginAt *node
	idx := 0

	if len(key) == 0 {
		// No explicit key provided by user, find the a leaf-node based on
		// scan direction and start there.
		if !reverse {
			beginAt, err = tree.leftLeaf(tree.root)
			idx = 0
		} else {
			beginAt, err = tree.rightLeaf(tree.root)
			idx = len(beginAt.entries) - 1
		}
	} else {
		// we have a specific key to start at. find the node containing the
		// key and start the scan there.
		beginAt, idx, _, err = tree.searchRec(tree.root, key)
	}

	if err != nil {
		return err
	}

	// starting at found leaf node, follow the 'next' pointer until.
	var nextNode int

	for beginAt != nil {
		if !reverse {
			for i := idx; i < len(beginAt.entries); i++ {
				e := beginAt.entries[i]
				if scanFn(e.key, e.val) {
					break
				}
			}
			nextNode = beginAt.next
		} else {
			for i := idx; i >= 0; i-- {
				e := beginAt.entries[i]
				if scanFn(e.key, e.val) {
					break
				}
			}
			nextNode = beginAt.prev
		}
		idx = 0

		if nextNode == 0 {
			break
		}

		beginAt, err = tree.fetch(nextNode)
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
	return fmt.Sprintf(
		"BPlusTree{file='%s', size=%d, degree=(%d, %d)}",
		tree.file, tree.Size(), tree.degree, tree.leafDegree,
	)
}

func (tree *BPlusTree) put(e entry) (bool, error) {
	if tree.isFull(tree.root) {
		// we will need 2 extra nodes for splitting the root
		// (1 to act as new root + 1 for the right sibling)
		nodes, err := tree.alloc(2)
		if err != nil {
			return false, err
		}

		newRoot := nodes[0]
		rightSibling := nodes[1]
		oldRoot := tree.root

		// update the tree root
		newRoot.children = append(newRoot.children, oldRoot.id)
		tree.root = newRoot
		tree.meta.rootID = uint32(newRoot.id)

		if err := tree.split(newRoot, oldRoot, rightSibling, 0); err != nil {
			return false, err
		}
	}

	return tree.insertNonFull(tree.root, e)
}

func (tree *BPlusTree) insertNonFull(n *node, e entry) (bool, error) {
	if len(n.children) == 0 {
		idx, found := n.search(e.key)

		if found {
			n.update(idx, e.val)
			return false, nil
		}

		n.insertAt(idx, e)
		return true, nil
	}

	return tree.insertInternal(n, e)
}

func (tree *BPlusTree) insertInternal(n *node, e entry) (bool, error) {
	idx, found := n.search(e.key)
	if found {
		idx++
	}

	child, err := tree.fetch(n.children[idx])
	if err != nil {
		return false, err
	}

	if tree.isFull(child) {
		sibling, err := tree.allocOne()
		if err != nil {
			return false, err
		}

		if err := tree.split(n, child, sibling, idx); err != nil {
			return false, err
		}

		// should go into left child or right child?
		if bytes.Compare(e.key, n.entries[idx].key) >= 0 {
			child, err = tree.fetch(n.children[idx+1])
			if err != nil {
				return false, err
			}
		}
	}

	return tree.insertNonFull(child, e)
}

func (tree *BPlusTree) split(p, n, sibling *node, i int) error {
	p.dirty = true
	n.dirty = true
	sibling.dirty = true

	if len(n.children) == 0 {
		// split leaf node. use 'sibling' as the right node for 'n'.
		sibling.next = n.next
		sibling.prev = n.id
		n.next = sibling.id

		sibling.entries = make([]entry, tree.leafDegree-1)
		copy(sibling.entries, n.entries[tree.leafDegree:])
		n.entries = n.entries[:tree.leafDegree]

		p.insertChild(i+1, sibling)
		p.insertAt(i, sibling.entries[0])
	} else {
		// split internal node. use 'sibling' as left node for 'n'.
		parentKey := n.entries[tree.degree-1]

		sibling.entries = make([]entry, tree.degree-1)
		copy(sibling.entries, n.entries[:tree.degree])
		n.entries = n.entries[tree.degree:]

		sibling.children = make([]int, tree.degree)
		copy(sibling.children, n.children[:tree.degree])
		n.children = n.children[tree.degree:]

		p.insertChild(i, sibling)
		p.insertAt(i, parentKey)
	}

	return nil
}

// searchRec searches the sub-tree with root 'n' recursively until the key
// is  found or the leaf node is  reached. Returns the node last searched,
// index where the key should be and a flag to indicate if the key exists.
func (tree *BPlusTree) searchRec(n *node, key []byte) (*node, int, bool, error) {
	idx, found := n.search(key)

	if n.isLeaf() {
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

// rightLeaf returns the right most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) rightLeaf(n *node) (*node, error) {
	if n.isLeaf() {
		return n, nil
	}

	lastChildIdx := len(n.children) - 1
	child, err := tree.fetch(n.children[lastChildIdx])
	if err != nil {
		return nil, err
	}

	return tree.rightLeaf(child)
}

// leftLeaf returns the left most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) leftLeaf(n *node) (*node, error) {
	if n.isLeaf() {
		return n, nil
	}

	child, err := tree.fetch(n.children[0])
	if err != nil {
		return nil, err
	}

	return tree.leftLeaf(child)
}

func (tree *BPlusTree) isFull(n *node) bool {
	if len(n.children) == 0 { // leaf node
		return len(n.entries) == ((2 * tree.leafDegree) - 1)
	}
	return len(n.entries) == ((2 * tree.degree) - 1)
}

// fetch returns the node with given id. underlying file is accessed
// only if the node doesn't exist in cache.
func (tree *BPlusTree) fetch(id int) (*node, error) {
	n, found := tree.nodes[id]
	if found {
		return n, nil
	}

	n = newNode(id, int(tree.meta.pageSz))
	if err := tree.pager.Unmarshal(id, n); err != nil {
		return nil, err
	}
	n.dirty = false
	tree.nodes[n.id] = n

	return n, nil
}

// allocOne allocates a page in the underlying pager and creates a node
// on that page. node is not written to the page in this call.
func (tree *BPlusTree) allocOne() (*node, error) {
	nodes, err := tree.alloc(1)
	if err != nil {
		return nil, err
	}
	return nodes[0], nil
}

// alloc allocates pages required for 'n' new nodes. alloc will reuse
// pages from free-list if available.
func (tree *BPlusTree) alloc(n int) ([]*node, error) {
	// check if there are enough free pages from the freelist
	// and try to allocate sequential set of pages.
	pid, rem := allocSeq(tree.meta.freeList, n)
	tree.meta.freeList = rem

	// free list could be having less pages than we actually need.
	// we need to allocate if that is the case.
	if pid < 0 {
		var err error
		pid, err = tree.pager.Alloc(n)
		if err != nil {
			return nil, err
		}
	}

	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		n := newNode(pid, int(tree.meta.pageSz))
		tree.nodes[pid] = n
		nodes[i] = n
		pid++
	}

	return nodes, nil
}

// open opens the B+ tree stored on disk using the pager. If the pager
// has no pages, a new B+ tree will be initialized.
func (tree *BPlusTree) open(opts Options) error {
	if tree.pager.Count() == 0 {
		// pager has no pages. initialize a new index.
		return tree.init(opts)
	}

	// we are opening an initialized index file. read page 0 as metadata.
	if err := tree.pager.Unmarshal(0, &tree.meta); err != nil {
		return err
	}

	// verify metadata
	if tree.meta.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", tree.meta.version, version)
	} else if tree.pager.PageSize() != int(tree.meta.pageSz) {
		return errors.New("page size in meta does not match pager")
	}

	// read the root node
	root, err := tree.fetch(int(tree.meta.rootID))
	if err != nil {
		return err
	}
	tree.root = root

	return nil
}

// init initializes a new B+ tree in the underlying file. allocates 2 pages
// (1 for meta + 1 for root) and initializes the instance. metadata and the
// root node are expected to be written to file during insertion.
func (tree *BPlusTree) init(opts Options) error {
	_, err := tree.pager.Alloc(2 + opts.PreAlloc)
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
		pageSz:   uint32(tree.pager.PageSize()),
		maxKeySz: uint16(opts.MaxKeySize),
	}

	tree.meta.freeList = make([]int, opts.PreAlloc)
	for i := 0; i < opts.PreAlloc; i++ {
		tree.meta.freeList[i] = i + 2 // +2 since first 2 pages reserved
	}

	return nil
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

// computeDegree computes the degree of the tree based on page-size and the
// maximum key size.
func (tree *BPlusTree) computeDegree(pageSz int) error {
	// available for node content in leaf/internal nodes
	leafContentSz := pageSz - leafNodeHeaderSz
	internalContentSz := pageSz - internalNodeHeaderSz

	const valueSz = 8       // for the uint64 value
	const childPtrSz = 4    // for uint32 child pointer in non-leaf node
	const keySizeSpecSz = 2 // for storing the actual key size

	leafEntrySize := int(valueSz + 2 + tree.meta.maxKeySz)
	internalEntrySize := int(childPtrSz + keySizeSpecSz + tree.meta.maxKeySz)

	// 4 bytes extra for the one extra child pointer
	tree.degree = (internalContentSz - 4) / (2 * internalEntrySize)
	tree.leafDegree = leafContentSz / (2 * leafEntrySize)

	if tree.leafDegree <= 2 || tree.degree <= 2 {
		return errors.New("invalid degree, reduce key size or increase page size")
	}
	return nil
}

// allocSeq finds a subset of size 'n' in 'free' that is sequential.
// Returns the first int in the sequence the set after removing the
// subset.
func allocSeq(free []int, n int) (id int, remaining []int) {
	if len(free) <= n {
		return -1, free
	} else if n == 1 {
		return free[0], free[1:]
	}

	i, j := 0, 0
	for ; i < len(free); i++ {
		j = i + (n - 1)
		if j < len(free) && free[j] == free[i]+(n-1) {
			break
		}
	}

	if i >= len(free) || j >= len(free) {
		return -1, free
	}

	id = free[i]
	free = append(free[:i], free[j+1:]...)
	return id, free
}
