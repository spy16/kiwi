package index

import (
	"encoding"
	"errors"
	"io"
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
	return &File{File: fh}, nil
}

// File is a wrapper around os.File and provides additional functions for
// dealing with files.
type File struct {
	*os.File
	data   mmap.MMap
	locked bool
}

// Size returns the size of the file.
func (f *File) Size() (int64, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// MMap memory maps the file to an internal bytes buffer.
func (f *File) MMap(flag int, lock bool) error {
	m, err := mmap.Map(f.File, flag, 0)
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
	return f.data.Unmap()
}

// BinaryWrite marshals and writes the data to the writer.
func BinaryWrite(f io.WriterAt, offset int64, m encoding.BinaryMarshaler) error {
	d, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = f.WriteAt(d, offset)
	return err
}

// BinaryRead reads data from the reader at offset and un-marshals using 'into'.
func BinaryRead(f io.ReaderAt, offset int64, size int, into encoding.BinaryUnmarshaler) error {
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		return err
	} else if n < size {
		return errors.New("read insufficient data")
	}
	return into.UnmarshalBinary(buf)
}
