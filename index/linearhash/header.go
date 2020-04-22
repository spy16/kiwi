package linearhash

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	// magic marker to indicate linear hash index.
	// hex version of 'lhash'
	magic   = uint32(0x6C686173)
	version = uint8(0x1) // indexer version

	headerSz = int(unsafe.Sizeof(header{}))
)

// header stores the information about the linear hash instance in the
// file.
// Note: Do not change the order of the fields.
type header struct {
	magic   uint32 // magic marker to indicate linear hash index file
	version uint8  // version of the linear hash indexer implementation
	flags   uint8  // control flags
	pageSz  uint16 // pageSize the index file was created with
	ptr     uint64 // base pointer to the remaining data in the page
}

func (h header) Validate() error {
	if h.magic != magic {
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
