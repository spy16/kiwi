package blob

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	// data file + data file header related constants
	dataFileMagic   = 0xB10B
	dataFileVersion = 0x01
	headerSize      = uint64(unsafe.Sizeof(dataFileHeader{}))

	// blob type related constants
	blobMagic    = uint16(0xBB)
	blobInUse    = 0x01
	blobHeaderSz = uint64(unsafe.Sizeof(blob{}) - unsafe.Sizeof(blob{}.ptr))
)

func headerFrom(buf []byte) *dataFileHeader {
	return (*dataFileHeader)(unsafe.Pointer(&buf[0]))
}

func blobFrom(buf []byte, offset uint64) *blob {
	return (*blob)(unsafe.Pointer(&buf[offset]))
}

type blob struct {
	magic uint16  // used for marking beginning of a blob
	flags uint16  // flags to indicate state (in-use, deleted etc)
	size  uint32  // size of the data in this blob
	ptr   [1]byte // pointer to remaining bytes
}

func (b *blob) setData(data []byte) {
	buf := (*[0x7FFFFFF]byte)(unsafe.Pointer(&b.ptr))
	copy(buf[:], data)
}

func (b *blob) getData() []byte {
	buf := (*[0x7FFFFFF]byte)(unsafe.Pointer(&b.ptr))
	return append([]byte(nil), buf[:b.size]...)
}

type dataFileHeader struct {
	magic   uint16
	version uint16
	count   uint32
	_       uint64 // freeList offset, not used but kept for future
}

func (h *dataFileHeader) validate() error {
	if h.magic != dataFileMagic {
		return errors.New("invalid magic")
	}
	if h.version != dataFileVersion {
		return fmt.Errorf("invalid/incompatible blob file version: %d", h.version)
	}
	return nil
}
