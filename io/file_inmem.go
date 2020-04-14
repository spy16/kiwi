package io

import (
	"errors"
	"io"
	"os"
	"sync"
)

// InMemoryFilePath can be passed as filePath to OpenFile to create an
// in-memory file instance.
const InMemoryFilePath = ":memory:"

var _ File = (*InMemory)(nil)

// InMemory implements an ephemeral file using in-memory byte slice.
type InMemory struct {
	mu       *sync.RWMutex
	data     []byte
	closed   bool
	readOnly bool
}

// ReadAt reads the data at given offset and copies it into 'p'.
func (mem *InMemory) ReadAt(p []byte, off int64) (n int, err error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	if len(p) == 0 {
		return 0, nil
	} else if off < 0 {
		return 0, &os.PathError{
			Op:   "readat",
			Path: InMemoryFilePath,
			Err:  errors.New("negative offset"),
		}
	} else if err := mem.canMutate("readat"); err != nil {
		return 0, err
	} else if int(off) >= len(mem.data) {
		return 0, io.EOF
	}

	sz := len(mem.data[off:])
	copy(p, mem.data[off:])

	if len(p) > sz {
		return sz, nil
	}
	return len(p), nil
}

// WriteAt writes the data from 'p' to in-memory file upto file size.
func (mem *InMemory) WriteAt(p []byte, off int64) (n int, err error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	if len(p) == 0 {
		return 0, nil
	} else if off < 0 {
		return 0, &os.PathError{
			Op:   "writeat",
			Path: InMemoryFilePath,
			Err:  errors.New("negative offset"),
		}
	} else if err := mem.canMutate("writeat"); err != nil {
		return 0, err
	}

	if len(mem.data) < int(off)+len(p) {
		if err := mem.truncate(int(off) + len(p)); err != nil {
			return 0, err
		}
	}

	copy(mem.data[off:], p)
	return len(p), nil
}

// Close marks the file as closed. other operations are invalid after
// close.
func (mem *InMemory) Close() error {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.closed = true
	mem.data = nil
	return nil
}

// Truncate resizes the file to given size.
func (mem *InMemory) Truncate(size int64) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	return mem.truncate(int(size))
}

// Size returns the size of the in-memory file. If the file is closed,
// always returns 0.
func (mem *InMemory) Size() (int64, error) {
	return int64(len(mem.data)), nil
}

// Name returns the InMemoryFilePath value.
func (mem *InMemory) Name() string { return InMemoryFilePath }

// MMap is a no-op.
func (mem *InMemory) MMap(flag int, lock bool) error { return nil }

// MUnmap is a no-op.
func (mem *InMemory) MUnmap() error { return nil }

func (mem *InMemory) truncate(size int) error {
	if err := mem.canMutate("truncate"); err != nil {
		return err
	}

	if len(mem.data) > int(size) {
		mem.data = mem.data[0:size]
	} else {
		mem.data = append(mem.data, make([]byte, size-len(mem.data))...)
	}

	return nil
}

func (mem *InMemory) canMutate(op string) error {
	if mem.readOnly {
		return &os.PathError{
			Op:   op,
			Path: InMemoryFilePath,
			Err:  errors.New("read-only file"),
		}
	} else if mem.closed {
		return errors.New("closed file")
	}

	return nil
}
