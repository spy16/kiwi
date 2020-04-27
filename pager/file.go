package pager

import (
	"errors"
	"io"
	"os"
)

var (
	_ RandomAccessFile = (*inMemory)(nil)
	_ RandomAccessFile = (*os.File)(nil)
)

// RandomAccessFile represents a file-like object that can be read from and
// written to at any offset.
type RandomAccessFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Truncate(size int64) error
	Name() string
}

type sizedFile interface {
	RandomAccessFile
	Size() int64
}

// inMemory implements an in-memory random access file.
type inMemory struct {
	closed   bool
	data     []byte
	readOnly bool
}

func (mem *inMemory) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	} else if off < 0 {
		return 0, &os.PathError{
			Op:   "readat",
			Path: InMemoryFileName,
			Err:  errors.New("negative offset"),
		}
	} else if err := mem.canMutate("readat"); err != nil {
		return 0, err
	} else if int(off) >= len(mem.data) {
		return 0, io.EOF
	}

	return copy(p, mem.data[off:]), nil
}

func (mem *inMemory) WriteAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	} else if off < 0 {
		return 0, &os.PathError{
			Op:   "writeat",
			Path: InMemoryFileName,
			Err:  errors.New("negative offset"),
		}
	} else if err := mem.canMutate("writeat"); err != nil {
		return 0, err
	}

	spaceRequired := off + int64(len(p))
	if int(spaceRequired) > len(mem.data) {
		_ = mem.Truncate(spaceRequired)
	}

	n = copy(mem.data[off:], p)
	return n, nil
}

func (mem *inMemory) Close() error {
	mem.closed = true
	mem.data = nil
	return nil
}

func (mem *inMemory) Truncate(size int64) error {
	d := mem.data
	mem.data = make([]byte, size)
	copy(mem.data, d)
	return nil
}

func (mem *inMemory) Size() int64 {
	return int64(len(mem.data))
}

func (mem *inMemory) Name() string {
	return InMemoryFileName
}

func (mem *inMemory) canMutate(op string) error {
	if mem.readOnly {
		return &os.PathError{
			Op:   op,
			Path: InMemoryFileName,
			Err:  errors.New("read-only file"),
		}
	} else if mem.closed {
		return errors.New("closed file")
	}

	return nil
}

func findSize(f RandomAccessFile) (int64, error) {
	switch file := f.(type) {
	case *os.File:
		stat, err := file.Stat()
		if err != nil {
			return 0, err
		}
		return stat.Size(), nil

	case sizedFile:
		return file.Size(), nil
	}

	return 0, errors.New("failed to find file size")
}
