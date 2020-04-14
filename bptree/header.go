package bptree

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	magic   = 0xABCDEF
	version = 0x1

	headerSz = int(unsafe.Sizeof(header{}))
)

type header struct {
	magic   uint32
	version uint16
	flags   uint16
	order   uint16
	pageSz  uint16
}

func (h header) Validate() error {
	if h.magic != magic {
		return fmt.Errorf("invalid magic: %#x", h.magic)
	}

	if h.version != version {
		return fmt.Errorf("incompatible/unknown version: %#x", h.version)
	}

	return nil
}

func (h *header) MarshalBinary() ([]byte, error) {
	if h == nil {
		return nil, errors.New("can't marshal nil header")
	}
	b := (*[]byte)(unsafe.Pointer(&h))
	return (*b)[0:headerSz], nil
}

func (h *header) UnmarshalBinary(d []byte) error {
	if h == nil {
		return errors.New("can't unamarshal into nil header")
	} else if len(d) < headerSz {
		return fmt.Errorf("need at-least %d bytes, got only %d", headerSz, len(d))
	}

	*h = (*[0xFFFFFF]header)(unsafe.Pointer(&d[0]))[0]
	return nil
}
