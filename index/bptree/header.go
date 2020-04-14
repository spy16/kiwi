package bptree

import (
	"errors"
	"fmt"
	"unsafe"
)

const headerSz = int(unsafe.Sizeof(header{}))

type header struct {
	magic   uint32 // magic marker to identify index file
	version uint8  // version of the indexer implementation
	flags   uint8  // any control flags (not used)
	pageSz  uint16 // pageSize used while initializing the db.
	degree  uint16 // degree of the B+ tree
	root    uint64 // pointer to root node page
}

func (h header) Validate() error {
	if h.magic != magic {
		return fmt.Errorf("invalid magic: %#x", h.magic)
	}

	if h.version != version {
		return fmt.Errorf("incompatible/unknown version: %#x", h.version)
	}

	if h.pageSz == 0 {
		return fmt.Errorf("invalid page size: %d", h.pageSz)
	}

	if h.degree == 0 {
		return fmt.Errorf("invalid tree order: %d", h.degree)
	}

	return nil
}

func (h header) MarshalBinary() ([]byte, error) {
	mem := (*[0xFFFF]byte)(unsafe.Pointer(&h))
	return append([]byte(nil), (*mem)[0:headerSz]...), nil
}

func (h *header) UnmarshalBinary(d []byte) error {
	if h == nil {
		return errors.New("cannot unamarshal into nil header")
	} else if len(d) < headerSz {
		return fmt.Errorf("need at-least %d bytes, got only %d", headerSz, len(d))
	}

	headers := (*[0xFFFFF]header)(unsafe.Pointer(&d[0]))
	*h = (*headers)[0]
	return nil
}
