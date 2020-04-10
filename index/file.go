package index

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

// Memory mapping flags.
const (
	RDONLY = mmap.RDONLY
	RDWR   = mmap.RDWR
	COPY   = mmap.COPY
	EXEC   = mmap.EXEC
)

// OpenFile uses os.OpenFile with given flags and returns the wrapped File
// instance.
func OpenFile(filePath string, flag int, mode os.FileMode) (*File, error) {
	fh, err := os.OpenFile(filePath, flag, mode)
	if err != nil {
		return nil, err
	}
	return &File{fh: fh}, nil
}

// File is a wrapper around os.File and provides additional functions for
// dealing with files.
type File struct {
	fh     *os.File
	locked bool
	data   mmap.MMap
}

// Truncate truncates the file to given size. Truncate frees the memory
// mapping before resize.
func (f *File) Truncate(size int64) error {
	_ = f.MUnmap()
	return f.fh.Truncate(size)
}

func (f *File) ReadAt(b []byte, offset int64) (int, error) {
	if f.data == nil {
		return f.fh.ReadAt(b, offset)
	}
	return copy(b, f.data[offset:]), nil
}

func (f *File) WriteAt(b []byte, offset int64) (int, error) {
	if f.data == nil {
		return f.fh.WriteAt(b, offset)
	}
	return copy(f.data[offset:], b), nil
}

// Close flushes any pending write in case of memory mapped file, and
// closes the underlying file handle.
func (f *File) Close() error {
	_ = f.MUnmap()
	return f.fh.Close()
}

// Size returns the size of the file.
func (f *File) Size() (int64, error) {
	stat, err := f.fh.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// MMap memory maps the file to an internal bytes buffer.
func (f *File) MMap(flag int, lock bool) error {
	m, err := mmap.Map(f.fh, flag, 0)
	if err != nil {
		return err
	}
	f.data = m
	if lock {
		_ = f.data.Lock()
	}
	return nil
}

// Unmap flushes any pending writes and frees the memory mapped region.
func (f *File) MUnmap() error {
	if f.locked {
		_ = f.data.Unlock()
	}
	err := f.data.Unmap()
	f.data = nil
	return err
}
