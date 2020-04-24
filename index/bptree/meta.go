package bptree

import (
	"errors"
	"unsafe"
)

const (
	version = uint8(0x1)

	metadataSz = int(unsafe.Sizeof(metadata{}))
)

// metadata represents the metadata for the B+ tree stored in a file.
type metadata struct {
	// temporary state info
	dirty bool

	// actual metadata
	version  uint8
	flags    uint8
	maxKeySz uint16
	pageSz   uint32
	size     uint32
	rootID   uint32
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSz)
	buf[0] = m.version
	buf[1] = m.flags
	bin.PutUint16(buf[2:4], m.maxKeySz)
	bin.PutUint32(buf[4:8], m.pageSz)
	bin.PutUint32(buf[8:12], m.size)
	bin.PutUint32(buf[12:16], m.rootID)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(d []byte) error {
	if len(d) < metadataSz {
		return errors.New("in-sufficient data for unmarshal")
	} else if m == nil {
		return errors.New("cannot unmarshal into nil")
	}

	m.version = d[0]
	m.flags = d[1]
	m.maxKeySz = bin.Uint16(d[2:4])
	m.pageSz = bin.Uint32(d[4:8])
	m.size = bin.Uint32(d[8:12])
	m.rootID = bin.Uint32(d[12:16])
	return nil
}
