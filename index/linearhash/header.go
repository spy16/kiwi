package linearhash

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	kiwiMagic = uint32(0x6B697769) // magic marker
	version   = uint16(0x1)        // indexer version

	headerSz = int(unsafe.Sizeof(header{}))
)

type header struct {
	magic   uint32
	version uint16
	pageSz  uint16
}

func (h header) Validate() error {
	if h.magic != kiwiMagic {
		return errors.New("invalid kiwi magic in header")
	}

	if h.version != version {
		return errors.New("invalid db version in header")
	}

	if h.pageSz == 0 {
		return errors.New("page size not set in header")
	}

	return nil
}

func (h header) MarshalBinary() ([]byte, error) {
	b := make([]byte, headerSz)
	binary.BigEndian.PutUint32(b[0:4], h.magic)
	binary.BigEndian.PutUint16(b[4:6], h.version)
	binary.BigEndian.PutUint16(b[6:8], h.pageSz)
	return b, nil
}

func (h *header) UnmarshalBinary(data []byte) error {
	h.magic = binary.BigEndian.Uint32(data[0:4])
	h.version = binary.BigEndian.Uint16(data[4:6])
	h.pageSz = binary.BigEndian.Uint16(data[6:8])
	return nil
}
