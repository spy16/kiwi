package io

import (
	"fmt"
	"io"
	"os"
)

// BlockFile provides facilities for low-level paged I/O on memory mapped,
// random access files. BlockFile is NOT safe for concurrent use. BlockFile
// gives direct access to memory mapped region and incorrect usage can cause
// segfaults or unexpected behaviors.
type BlockFile interface {
	io.Closer

	// Alloc should allocate 'n' new sequential blocks and return the id of the
	// first block and slice pointer to the first block.
	Alloc(n int) (id int, slice []byte, err error)

	// Slice returns a slice of the memory mapped region starting at the block
	// with the given id. Alloc() calls may invalidate the returned slice. It
	// is caller's responsibility to co-ordinate Alloc() and Slice() calls.
	Slice(id int) ([]byte, error)

	// Info returns information about the block file state/configuration.
	Info() (name string, count, blockSz int, readOnly bool)
}

// Open opens the named file and returns a BlockFile instance for it. If the
// file doesn't exist, it will be created. If the fileName is ':memory:', an
// in-memory block-file will be returned.
func Open(fileName string, blockSz int, readOnly bool, mode os.FileMode) (BlockFile, error) {
	if fileName == ":memory:" {
		return &InMem{
			blockSz:  blockSz,
			readOnly: readOnly,
		}, nil
	}

	if (blockSz < 4096) || blockSz%4096 != 0 {
		return nil, fmt.Errorf("invalid blockSize, must be non-zero multiple of 4096")
	}

	return openOnDisk(fileName, blockSz, readOnly, mode)
}
