package bptree

import (
	"errors"
	"log"
)

const (
	magic              = 0xD0D
	version            = uint8(0x1)
	metadataHeaderSize = 16
)

// metadata represents the metadata for the B+ tree stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	magic    uint16 // magic marker to identify B+ tree.
	version  uint8  // version of implementation
	flags    uint8  // flags (unused)
	maxKeySz uint16 // maximum key size allowed
	pageSz   uint32 // page size used to initialize
	size     uint32 // number of entries in the tree
	rootID   uint32 // page id for the root node
	freeList []int  // list of allocated, unused pages
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, m.pageSz)

	// verify that the free list can fit inside the meta page.
	freeListSpace := int(m.pageSz) - metadataHeaderSize
	if len(m.freeList)*4 > freeListSpace {
		// TODO: make sure this doesn't happen by compacting pager
		// when free page count hits a threshold
		log.Printf("WARNING: truncating free list since it doesn't fit in meta page")
		m.freeList = m.freeList[:freeListSpace/4]
	}

	bin.PutUint16(buf[0:2], m.magic)
	buf[2] = m.version
	buf[3] = m.flags
	bin.PutUint16(buf[4:6], m.maxKeySz)
	bin.PutUint32(buf[6:10], m.pageSz)
	bin.PutUint32(buf[10:14], m.size)
	bin.PutUint32(buf[14:18], m.rootID)
	bin.PutUint32(buf[18:22], uint32(len(m.freeList)))

	offset := 21
	for i := 0; i < len(m.freeList); i++ {
		bin.PutUint32(buf[offset:offset+4], uint32(m.freeList[i]))
		offset += 4
	}

	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	if len(d) < metadataHeaderSize {
		return errors.New("in-sufficient data for unmarshal")
	} else if m == nil {
		return errors.New("cannot unmarshal into nil")
	}

	m.magic = bin.Uint16(d[0:2])
	m.version = d[2]
	m.flags = d[3]
	m.maxKeySz = bin.Uint16(d[4:6])
	m.pageSz = bin.Uint32(d[6:10])
	m.size = bin.Uint32(d[10:14])
	m.rootID = bin.Uint32(d[14:18])

	m.freeList = make([]int, bin.Uint32(d[18:22]))
	offset := 22
	for i := 0; i < len(m.freeList); i++ {
		m.freeList[i] = int(bin.Uint32(d[offset : offset+4]))
		offset += 4
	}

	return nil
}
