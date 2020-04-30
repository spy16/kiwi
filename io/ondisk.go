package io

import (
	"errors"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
)

var _ BlockFile = (*OnDisk)(nil)

// disableMmap disables memory mapped I/O in OnDisk block file. meant for
// debugging only.
const disableMmap = false

func openOnDisk(fileName string, blockSz int, readOnly bool, mode os.FileMode) (*OnDisk, error) {
	var bf OnDisk

	if blockSz == 0 {
		blockSz = os.Getpagesize()
	} else if blockSz < 4096 || blockSz%4096 != 0 {
		return nil, errors.New("block size must be multple of 4096")
	}

	mmapFlag := mmap.RDWR
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		mmapFlag = mmap.RDONLY
		flag = os.O_CREATE | os.O_RDONLY
	}

	f, err := os.OpenFile(fileName, flag, mode)
	if err != nil {
		return nil, err
	}

	bf = OnDisk{
		file:      f,
		readOnly:  readOnly,
		blockSize: blockSz,
		mmapFlag:  mmapFlag,
	}

	fi, err := f.Stat()
	if err != nil {
		_ = bf.Close()
		return nil, err
	}
	bf.size = fi.Size()

	if err := bf.mmap(); err != nil {
		_ = bf.Close()
		return nil, err
	}

	return &bf, nil
}

// OnDisk implements a memory mapped BlockFile using an on-disk file.
type OnDisk struct {
	file      *os.File
	data      mmap.MMap
	size      int64
	readOnly  bool
	mmapFlag  int
	blockSize int
}

// Slice returns a slice of the memory mapped region starting at the block
// with the given id. Incorrect handling of the returned slice can cause
// segfaults or unexpected behavior. Any Alloc() calls may invalidate the
// returned slice.
func (bf *OnDisk) Slice(id int) ([]byte, error) {
	off := int64(bf.offset(id))

	if id < 0 || off >= bf.size {
		return nil, io.EOF
	} else if bf.file == nil || bf.data == nil {
		return nil, os.ErrClosed
	}

	return bf.data[off:], nil
}

// Alloc will allocate 'n' sequential blocks and return the first id and
// slice to the first block.
func (bf *OnDisk) Alloc(n int) (int, []byte, error) {
	id := int(bf.size) / bf.blockSize

	targetSz := bf.size + int64(n*bf.blockSize)
	_ = bf.unmap()
	if err := bf.file.Truncate(targetSz); err != nil {
		return 0, nil, err
	}

	bf.size = targetSz
	if err := bf.mmap(); err != nil {
		return 0, nil, err
	}

	sl, err := bf.Slice(id)
	return id, sl, err
}

// Info returns information about the block file state/configuration.
func (bf *OnDisk) Info() (name string, count, blockSz int, readOnly bool) {
	return bf.file.Name(), int(bf.size) / bf.blockSize, bf.blockSize, bf.readOnly
}

// Close flushes any pending writes and closes the underlying file.
func (bf *OnDisk) Close() error {
	if bf.file == nil {
		return nil
	}
	_ = bf.unmap()
	err := bf.file.Close()
	bf.file = nil
	return err
}

func (bf *OnDisk) mmap() error {
	if disableMmap || bf.file == nil || bf.size <= 0 {
		return nil
	}

	if err := bf.unmap(); err != nil {
		return err
	}

	d, err := mmap.Map(bf.file, bf.mmapFlag, 0)
	if err != nil {
		return err
	}
	bf.data = d
	return nil
}

func (bf *OnDisk) unmap() error {
	if bf.file == nil || bf.data == nil {
		return nil
	}
	return bf.data.Unmap()
}

func (bf *OnDisk) offset(id int) int { return id * bf.blockSize }
