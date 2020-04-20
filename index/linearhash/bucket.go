package linearhash

import (
	"errors"
	"fmt"
	"unsafe"
)

const bucketSz = int(unsafe.Sizeof(bucket{}))

type bucket struct {
	id       uint32 // id of this bucket
	overflow uint32 // overflow bucket id. (0 means no overflow)
	flags    uint32 // control flags
	ptr      uint32 // dummy pointer to slots
}

func (b bucket) next(idx *LinearHash) (*bucket, error) {
	if b.overflow == 0 {
		return nil, nil
	}

	nextBucket := bucket{}
	if err := idx.pager.Unmarshal(int(b.overflow), &nextBucket); err != nil {
		return nil, err
	}

	return &nextBucket, nil
}

func (b *bucket) slot(id int) *slot {
	slots := (*[0xFFFF]slot)(unsafe.Pointer(&b.ptr))
	return &(slots)[id]
}

func (b bucket) Validate() error {
	return nil
}

func (b bucket) MarshalBinary() ([]byte, error) {
	mem := (*[0xFFFF]byte)(unsafe.Pointer(&b))
	return append([]byte(nil), (*mem)[0:bucketSz]...), nil
}

func (b *bucket) UnmarshalBinary(d []byte) error {
	if b == nil {
		return errors.New("cannot unamarshal into nil slot")
	} else if len(d) < bucketSz {
		return fmt.Errorf("need at-least %d bytes, got only %d", bucketSz, len(d))
	}

	buckets := (*[0xFFFFF]bucket)(unsafe.Pointer(&d[0]))
	*b = (*buckets)[0]
	return nil
}

type slot struct {
	hash     uint64
	blobID   uint64
	keySz    uint32
	checksum uint32
}
