package pager

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const version = 0x1
const headerSz = int(unsafe.Sizeof(header{}))

type header struct {
	magic    uint32 // magic pager was initialized with.
	version  uint8  // pager implementation version
	flags    uint8  // control flags (not used)
	pageSize uint16 // page size this page file was initialized with.
}

func (h header) MarshalBinary() (data []byte, err error) {
	buf := make([]byte, headerSz)
	binary.LittleEndian.PutUint32(buf[0:4], h.magic)
	buf[4] = h.version
	buf[5] = h.flags
	binary.LittleEndian.PutUint16(buf[6:8], h.pageSize)
	return buf, nil
}

func (h *header) UnmarshalBinary(data []byte) error {
	if len(data) < headerSz {
		return errors.New("in-sufficient data to unmarshal")
	} else if h == nil {
		return errors.New("cannot unmarshal into nil-header")
	}

	h.magic = binary.LittleEndian.Uint32(data[0:4])
	h.version = data[4]
	h.flags = data[5]
	h.pageSize = binary.LittleEndian.Uint16(data[6:8])
	return nil
}
