package linearhash

import (
	"errors"
	"fmt"
)

const (
	// kiwiMagic is the marker to indicate the index file is a valid kiwi
	// file.
	kiwiMagic   uint32 = 0x6B697769
	kiwiVersion uint32 = 0x01
)

type header struct {
	magic       uint32
	version     uint32
	pageSz      uint32
	slotCount   uint32
	hashSeed    uint32
	bucketCount uint32
}

func (h *header) validate() error {
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
