package blob

import (
	"io"
	"unsafe"
)

const pageHeaderSz = int(unsafe.Sizeof(pageHeader{}))

// Blob represents a blob of binary data composed of multiple pages.
type Blob struct {
	id        int
	pageSize  int    // size of one page
	totalSize int    // total size of the blob
	readSize  int    // total data read
	data      []byte // data that has been read

	pager interface {
		alloc() (int, error)
		read(id int) ([]byte, error)
		write(id int, d []byte) error
	}
}

// ID returns the id of this blob record which is same as the id of the
// first page.
func (b Blob) ID() int { return b.id }

// Size returns the total blob size.
func (b Blob) Size() int { return b.totalSize }

// ReadAt reads the data from the blob into the given buffer. Actual file
// read is always done in pages.
func (b Blob) ReadAt(buf []byte, offset int64) (n int, err error) {
	if len(buf) == 0 {
		return 0, nil
	} else if int(offset) >= b.totalSize {
		return 0, io.EOF
	}

	return 0, nil
}

// Flush flushes the data to the underlying file.
func (b *Blob) Flush() error { return nil }

type page struct {
	id    int
	data  []byte
	dirty bool // has unsaved changes?
}

type pageHeader struct {
	nextPage    uint32 // pointer to next page
	prevPage    uint32 // pointer to previous page
	flags       uint16 // deleted etc
	contentSize uint16 // size of actual content
}
