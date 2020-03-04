package linearhash

import (
	"errors"
	"io"
	"unsafe"
)

const (
	bucketMagic uint8 = 0xBA

	// bucket flags
	bucketHasOverflow = 0x1
	bucketIsOverflow  = 0x2

	bucketSz = uint32(unsafe.Sizeof(bucket{}))
	slotSz   = uint32(unsafe.Sizeof(slot{}))
)

type bucket struct {
	magic    uint8   // magic to identify validity of the bucket
	flags    uint8   // flags to provide info about the bucket
	overflow uint64  // overflow bucket offset in blob-store
	ptr      uintptr // pointer to slots
}

type slot struct {
	hash       uint32 // hash value of the key
	keySz      uint16 // size of the key
	valSz      uint32 // size of the value
	blobOffset uint64 // offset for key-value in the blob store
}

type bucketCursor struct {
	head *bucket
	cur  *bucket
	blob BlobStore
}

func (b *bucket) slot(id int) *slot {
	slots := (*[0x7FFFFFF]slot)(unsafe.Pointer(&b.ptr))
	return &(slots)[id]
}

func (b *bucket) validate() error {
	if b.magic != bucketMagic {
		return errors.New("invalid bucket magic")
	}

	return nil
}

func (bc *bucketCursor) Next() error {
	if bc.cur == nil {
		bc.cur = bc.head
		return nil
	}

	if bc.cur.flags&bucketHasOverflow == 0 {
		// has no overflow bucket
		return io.EOF
	}

	d, err := bc.blob.Fetch(bc.cur.overflow)
	if err != nil {
		return err
	}

	b := (*bucket)(unsafe.Pointer(&d[0]))
	bc.cur = b
	return b.validate()
}

func (bc *bucketCursor) ForEach(cb func(b *bucket) (stop bool, err error)) (err error) {
	for err = bc.Next(); err == nil; err = bc.Next() {
		stop, cbErr := cb(bc.cur)
		if cbErr != nil || stop {
			return cbErr
		}
	}

	return err
}
