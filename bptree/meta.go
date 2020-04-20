package bptree

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const metadataSz = int(unsafe.Sizeof(metadata{}))

var bin = binary.LittleEndian

type metadata struct {
	version  uint8
	flags    uint8
	maxKeySz uint16
	pageSz   uint16
	size     uint32
	root     uint32
}

func (m metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metadataSz)
	buf[0] = m.version
	buf[1] = m.flags
	bin.PutUint16(buf[2:4], m.maxKeySz)
	bin.PutUint16(buf[4:6], m.pageSz)
	bin.PutUint32(buf[6:10], m.size)
	bin.PutUint32(buf[10:14], m.root)
	return nil, nil
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
	m.pageSz = bin.Uint16(d[4:6])
	m.size = bin.Uint32(d[6:10])
	m.root = bin.Uint32(d[10:14])
	return nil
}
