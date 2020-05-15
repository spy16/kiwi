package bptree

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	leafNodeHeaderSz     = 11
	internalNodeHeaderSz = 3

	flagLeafNode     = uint8(0x0)
	flagInternalNode = uint8(0x1)
)

// newNode initializes an in-memory leaf node and returns.
func newNode(id int, pageSz int) *node {
	return &node{
		id:    id,
		dirty: true,
	}
}

// node represents an internal or leaf node in the B+ tree.
type node struct {
	// configs for read/write
	dirty bool

	// node data
	id       int
	next     int
	prev     int
	entries  []entry
	children []int
}

// search performs a binary search in the node entries for the given key
// and returns the index where it should be and a flag indicating whether
// key exists.
func (n node) search(key []byte) (idx int, found bool) {
	lo, hi := 0, len(n.entries)-1

	var mid int
	for lo <= hi {
		mid = (hi + lo) / 2

		cmp := bytes.Compare(key, n.entries[mid].key)
		switch {
		case cmp == 0:
			return mid, true

		case cmp > 0:
			lo = mid + 1

		case cmp < 0:
			hi = mid - 1
		}
	}

	return lo, false
}

// insertChild adds the given child at appropriate location under the node.
func (n *node) insertChild(idx int, child *node) {
	n.dirty = true
	n.children = append(n.children, 0)
	copy(n.children[idx+1:], n.children[idx:])
	n.children[idx] = child.id
}

// insertAt inserts the entry at the given index into the node.
func (n *node) insertAt(idx int, e entry) {
	n.dirty = true
	n.entries = append(n.entries, entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = e
}

// removeAt removes the entry at given index and returns the value
// that existed.
func (n *node) removeAt(idx int) entry {
	n.dirty = true
	e := n.entries[idx]
	n.entries = append(n.entries[:idx], n.entries[idx:]...)
	return e
}

// update updates the value of the entry with given index.
func (n *node) update(entryIdx int, val uint64) {
	if val != n.entries[entryIdx].val {
		n.dirty = true
		n.entries[entryIdx].val = val
	}
}

// isLeaf returns true if this node has no children. (i.e., it is
// a leaf node.)
func (n node) isLeaf() bool { return len(n.children) == 0 }

func (n node) String() string {
	s := "{"
	for _, e := range n.entries {
		s += fmt.Sprintf("'%s' ", e.key)
	}
	s += "} "
	s += fmt.Sprintf(
		"[id=%d, size=%d, leaf=%t, %d<-n->%d]",
		n.id, len(n.entries), n.isLeaf(), n.prev, n.next,
	)

	return s
}

func (n node) size() int {
	if n.isLeaf() {
		sz := leafNodeHeaderSz
		for i := 0; i < len(n.entries); i++ {
			// 2 for the key size, 8 for the uint64 value
			sz += 2 + 8 + len(n.entries[i].key)
		}
		return sz

	}

	sz := internalNodeHeaderSz + 4 // +4 for the extra child pointer
	for i := 0; i < len(n.entries); i++ {
		// 4 for the child pointer, 2 for the key size
		sz += 4 + 2 + len(n.entries[i].key)
	}
	return sz
}

func (n node) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.size())
	offset := 0

	if n.isLeaf() {
		// Note: update leafNodeHeaderSz if this is updated.
		buf[offset] = flagLeafNode
		offset++

		bin.PutUint16(buf[offset:offset+2], uint16(len(n.entries)))
		offset += 2

		bin.PutUint32(buf[offset:offset+4], uint32(n.next))
		offset += 4

		bin.PutUint32(buf[offset:offset+4], uint32(n.prev))
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
	if n == nil {
		return errors.New("cannot unmarshal into nil node")
	}

	offset := 1 // (skip 0th field for flag)
	if d[0]&flagInternalNode == 0 {
		// leaf node
		entryCount := int(bin.Uint16(d[offset : offset+2]))
		offset += 2

		n.next = int(bin.Uint32(d[offset : offset+4]))
		offset += 4

		n.prev = int(bin.Uint32(d[offset : offset+4]))
		offset += 4

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
