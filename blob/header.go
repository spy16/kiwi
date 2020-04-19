package blob

import (
	"errors"
	"unsafe"
)

const blobHeaderSz = int(unsafe.Sizeof(blobHeader{}))

type blobHeader struct {
	flags     uint16 // control flags (deleted, is-head etc.)
	totalSize uint16 // total size of the blob
	size      uint16 // content size in this page
	next      uint32 // pointer to next page of the blob
}

func (h blobHeader) MarshalBinary() ([]byte, error) {
	buf := make([]byte, blobHeaderSz)
	bin.PutUint16(buf[0:2], h.flags)
	bin.PutUint16(buf[2:4], h.totalSize)
	bin.PutUint16(buf[4:6], h.size)
	bin.PutUint32(buf[6:10], h.next)
	return buf, nil
}

func (h *blobHeader) UnmarshalBinary(d []byte) error {
	if len(d) < blobHeaderSz {
		return errors.New("in-sufficient data to unmarshal header")
	} else if h == nil {
		return errors.New("cannot unmarshal into nil header")
	}

	h.flags = bin.Uint16(d[0:2])
	h.totalSize = bin.Uint16(d[2:4])
	h.size = bin.Uint16(d[4:6])
	h.next = bin.Uint32(d[6:10])
	return nil
}
