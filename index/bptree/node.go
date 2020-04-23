package bptree

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

const (
	leafNodeHeaderSz     = 6
	internalNodeHeaderSz = 3

	flagLeafNode     = uint8(0x0)
	flagInternalNode = uint8(0x1)
)

// newNode initializes an in-memory leaf node and returns.
func newNode(id int, pageSz int) *node {
	return &node{
		id:     id,
		dirty:  true,
		pageSz: pageSz,
	}
}

// node represents an internal or leaf node in the B+ tree.
type node struct {
	// configs for read/write
	dirty  bool
	pageSz int

	// node data
	id       int
	next     int
	entries  []entry
	children []int
}

// Search performs a binary search in the node entries for the given key
// and returns the index where it should be and a flag indicating whether
// key exists.
func (n node) Search(key []byte) (idx int, found bool) {
	L := len(n.entries)

	idx = sort.Search(L, func(i int) bool {
		return bytes.Compare(key, n.entries[i].key) != 1
	})
	found = (idx < L) && bytes.Equal(n.entries[idx].key, key)

	return idx, found
}

// SplitRight creates a new node to act as right sibling of this node
// and moves the right half of this node to it. Both nodes will be marked
// dirty after the split and need to be written to disk.
func (n *node) SplitRight(rightInto *node) {
	size := len(n.entries)

	if n.IsLeaf() {
		at := (size - 1) / 2
		rightInto.entries = make([]entry, size-at)
		copy(rightInto.entries, n.entries[at:])
		n.entries = n.entries[:at]

		rightInto.next = n.next // right node now points to the next of 'n'
		n.next = rightInto.id   // left node points to 'right'
	} else {
		at := (size / 2) + 1

		rightInto.entries = append([]entry(nil), n.entries[at:]...)
		rightInto.children = append([]int(nil), n.children[at:len(n.entries)+1]...)

		n.entries = n.entries[:at-1]
		n.children = n.children[:at]
	}

	// both nodes have changed and need to be written to file
	rightInto.dirty = true
	n.dirty = true
}

// AddChild adds the given child at appropriate location under the node.
func (n *node) AddChild(key []byte, child *node) {
	n.dirty = true
	idx, found := n.Search(key)
	if found {
		n.children[idx+1] = child.id
		return
	}

	n.InsertAt(idx, entry{key: key})

	// insert the child node at idx
	n.children = append(n.children, 0)
	copy(n.children[idx+2:], n.children[idx+1:])
	n.children[idx+1] = child.id
}

// InsertAt inserts the entry at the given index into the node.
func (n *node) InsertAt(idx int, e entry) {
	n.dirty = true
	n.entries = append(n.entries, entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = e
}

// SetVal updates the value of the entry with given index.
func (n *node) SetVal(entryIdx int, val uint64) {
	if val != n.entries[entryIdx].val {
		n.dirty = true
		n.entries[entryIdx].val = val
	}
}

// IsLeaf returns true if this node has no childrent. (i.e., it is
// a leaf node.)
func (n node) IsLeaf() bool { return len(n.children) == 0 }

func (n node) String() string {
	s := fmt.Sprintf(
		"{id=%d, size=%d, leaf=%t, keys=[ ",
		n.id, len(n.entries), n.IsLeaf(),
	)
	for _, e := range n.entries {
		s += fmt.Sprintf("'%s' ", e.key)
	}
	s += fmt.Sprintf("], next=%d}", n.next)
	return s
}

func (n node) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.pageSz)
	offset := 0

	if n.IsLeaf() {
		// Note: update leafNodeHeaderSz if this is updated.
		buf[offset] = flagLeafNode
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(n.entries)))
		offset += 2

		bin.PutUint32(buf[offset:offset+4], uint32(n.next))
		offset += 4

		for i := 0; i < len(n.entries); i++ {
			e := n.entries[i]

			bin.PutUint64(buf[offset:offset+8], e.val)
			offset += 8

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.key)))
			offset += 2

			copy(buf[offset:], e.key)
			offset += len(e.key)
		}
	} else {
		// Note: update internalNodeHeaderSz if this is updated.
		buf[offset] = flagInternalNode
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(n.entries)))
		offset += 2

		// write the 0th pointer
		bin.PutUint32(buf[offset:offset+4], uint32(n.children[0]))
		offset += 4

		for i := 0; i < len(n.entries); i++ {
			e := n.entries[i]

			bin.PutUint32(buf[offset:offset+4], uint32(n.children[i+1]))
			offset += 4

			bin.PutUint16(buf[offset:offset+2], uint16(len(e.key)))
			offset += 2

			copy(buf[offset:], e.key)
			offset += len(e.key)
		}
	}
	return buf, nil
}

func (n *node) UnmarshalBinary(d []byte) error {
	if len(d) < n.pageSz {
		return errors.New("in-sufficient data")
	} else if n == nil {
		return errors.New("cannot unmarshal into nil node")
	}

	offset := 1 // (skip 0th field for flag)
	if d[0]&flagInternalNode == 0 {
		// leaf node
		entryCount := int(bin.Uint16(d[offset : offset+2]))
		offset += 2

		n.next = int(bin.Uint32(d[offset : offset+4]))
		offset += 4 // we are now at offset 7

		for i := 0; i < entryCount; i++ {
			e := entry{}
			e.val = bin.Uint64(d[offset : offset+8])
			offset += 8

			keySz := int(bin.Uint16(d[offset : offset+2]))
			offset += 2

			e.key = make([]byte, keySz)
			copy(e.key, d[offset:offset+keySz])
			offset += keySz

			n.entries = append(n.entries, e)
		}
	} else {
		// internal node
		entryCount := int(bin.Uint16(d[offset : offset+2]))
		offset += 2

		// read the left most child pointer
		n.children = append(n.children, int(bin.Uint32(d[offset:offset+4])))
		offset += 4 // we are at offset 7 now

		for i := 0; i < entryCount; i++ {
			childPtr := bin.Uint32(d[offset : offset+4])
			offset += 4

			keySz := bin.Uint16(d[offset : offset+2])
			offset += 2

			key := make([]byte, keySz)
			copy(key, d[offset:])
			offset += int(keySz)

			n.children = append(n.children, int(childPtr))
			n.entries = append(n.entries, entry{key: key})
		}

	}

	return nil
}

type entry struct {
	key []byte
	val uint64
}
