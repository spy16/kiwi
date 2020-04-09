package linearhash

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	// kiwiMagic is the marker to indicate the index file is a valid kiwi
	// file.
	kiwiMagic   uint32 = 0x6B697769
	kiwiVersion uint32 = 0x01
)

func headerFrom(buf []byte) *header {
	return (*header)(unsafe.Pointer(&buf[0]))
}

type header struct {
	magic       uint32 // magic marker to indicate valid header
	version     uint32 // version of the index file format
	pageSz      uint32 // pageSz the index file was created with
	bucketCount uint32 // number of buckets in the index
	splitBucket uint32 // index of the bucket that will be split next
}

func (h header) validate() error {
	if h.magic != kiwiMagic {
		return errors.New("invalid magic in header")
	}

	if h.version != kiwiVersion {
		return fmt.Errorf("invalid/incompatible version: %d", h.version)
	}

	if h.pageSz == 0 {
		return errors.New("invalid page size in header")
	}

	if h.bucketCount == 0 {
		// we initialize the db with at-least 1 bucket.
		return errors.New("invalid bucket count")
	}

	return nil
}

func (h header) slotCount() int {
	return int((h.pageSz - bucketSz) / slotSz)
}
