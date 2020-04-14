package io

import (
	"io"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

// Memory mapping flags. See MemoryMappedFile.
const (
	RDONLY = mmap.RDONLY
	RDWR   = mmap.RDWR
	COPY   = mmap.COPY
	EXEC   = mmap.EXEC
)

// OpenFile uses os.OpenFile with given flags and returns the wrapped File
// instance. If the filePath is ":memory:", an in-memory file (ephemeral)
// is returned.
func OpenFile(filePath string, flag int, mode os.FileMode) (File, error) {
	if filePath == InMemoryFilePath {
		return &InMemory{
			mu:       &sync.RWMutex{},
			closed:   false,
			readOnly: flag == 0,
		}, nil
	}

	fh, err := os.OpenFile(filePath, flag, mode)
	if err != nil {
		return nil, err
	}
	return &OnDisk{fh: fh}, nil
}

// File represents a file-like object. (An in-memory or on-disk).
type File interface {
	io.ReaderAt
	io.WriterAt
	Name() string
	Close() error
	Size() (int64, error)
	Truncate(size int64) error
	MMap(flag int, lock bool) error
	MUnmap() error
}
