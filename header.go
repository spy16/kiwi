package kiwi

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const headerSz = int(unsafe.Sizeof(header{}))

type header struct {
	magic   uint32
	version uint32
	backend BackendType
	pageSz  uint16
}

func (h header) Validate() error {
	if h.magic != kiwiMagic {
		return errors.New("invalid kiwi magic in header")
	}

	if h.version != dbVersion {
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
	binary.BigEndian.PutUint32(b[4:8], h.version)
	binary.BigEndian.PutUint16(b[8:10], uint16(h.backend))
	binary.BigEndian.PutUint16(b[10:12], h.pageSz)
	return b, nil
}

func (h *header) UnmarshalBinary(data []byte) error {
	h.magic = binary.BigEndian.Uint32(data[0:4])
	h.version = binary.BigEndian.Uint32(data[4:8])
	h.backend = BackendType(binary.BigEndian.Uint16(data[8:10]))
	h.pageSz = binary.BigEndian.Uint16(data[10:12])
	return nil
}
