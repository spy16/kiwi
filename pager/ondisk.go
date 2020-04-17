package pager

import (
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/spy16/kiwi/io"
)

func openOnDisk(filePath string, opts Options) (*OnDisk, error) {
	fh, err := os.OpenFile(filePath, opts.fileFlag, opts.FileMode)
	if err != nil {
		return nil, err
	}

	p := &OnDisk{
		file:     fh,
		magic:    opts.Magic,
		mmapFlag: opts.mmapFlag,
		readOnly: opts.ReadOnly,
	}

	if err := p.open(); err != nil {
		_ = p.Close()
		return nil, err
	}

	if err := p.mmap(); err != nil {
		_ = p.Close()
		return nil, err
	}

	return p, nil
}

// OnDisk represents a paged file instance and provides IO functions in
// a strictly paged manner.
type OnDisk struct {
	// file state
	file     *os.File
	data     mmap.MMap
	mmapFlag int
	readOnly bool
	fileSize int64

	// paging information
	magic    uint32
	pageSize int
}

// Alloc allocates 'count' number of new pages.
func (p *OnDisk) Alloc(count int) error {
	if count <= 0 {
		return errors.New("count must be positive non-zero")
	} else if p.file == nil {
		return os.ErrClosed
	}

	total := p.Count() + count
	if err := p.resize(total); err != nil {
		return err
	}

	return nil
}

// Read reads the page with given id from the file and returns the page
// data.
func (p *OnDisk) Read(id int) ([]byte, error) {
	if id >= p.Count() {
		return nil, errors.New("non-existent page")
	} else if p.file == nil {
		return nil, os.ErrClosed
	}

	return p.readAt(p.offset(id))
}

// Write writes the data into the page with given id.
func (p *OnDisk) Write(id int, d []byte) error {
	if len(d) > p.pageSize {
		return errors.New("data is larger than page size")
	} else if id >= p.Count() {
		return errors.New("non-existent page")
	} else if p.file == nil {
		return os.ErrClosed
	}

	return p.writeAt(d, p.offset(id))
}

// Count returns the current number of pages in the file.
func (p *OnDisk) Count() int {
	return int(math.Ceil(float64(int(p.fileSize) / p.pageSize)))
}

// Close flushes any pending writes and frees file.
func (p *OnDisk) Close() error {
	if p.file == nil {
		return nil
	}
	_ = p.unmmap()
	err := p.file.Close()
	p.file = nil
	return err
}

func (p *OnDisk) String() string {
	return fmt.Sprintf("Pager{file='%s',pageSize=%d, count=%d}",
		p.file.Name(), p.pageSize, p.Count())
}

func (p *OnDisk) readAt(offset int64) ([]byte, error) {
	if p.data == nil {
		return nil, os.ErrClosed
	} else if (int(offset) % p.pageSize) != 0 {
		return nil, errors.New("offset is not multiple of pagesize")
	}

	buf := make([]byte, p.pageSize)
	if _, err := p.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (p *OnDisk) writeAt(buf []byte, offset int64) error {
	if p.data == nil {
		return os.ErrClosed
	} else if (int(offset) % p.pageSize) != 0 {
		return errors.New("offset is not multiple of pagesize")
	}

	copy(p.data[offset:], buf)
	return nil
}

func (p *OnDisk) offset(id int) int64 {
	return int64((id + 1) * p.pageSize)
}

func (p *OnDisk) open() error {
	fi, err := p.file.Stat()
	if err != nil {
		return err
	}
	p.fileSize = fi.Size()

	if p.fileSize == 0 {
		return p.init()
	}

	h := header{}
	if err := io.BinaryRead(p.file, 0, 0x1000, &h); err != nil {
		return err
	}
	p.magic = h.magic
	p.pageSize = int(h.pageSize)
	return nil
}

func (p *OnDisk) init() error {
	p.pageSize = os.Getpagesize()
	if err := p.resize(1); err != nil {
		return err
	}

	h := header{
		magic:    p.magic,
		pageSize: uint16(os.Getpagesize()),
		version:  version,
	}
	return io.BinaryWrite(p.file, 0, h)
}

// resize resizes the file to have exactly 'count' number of pages.
func (p *OnDisk) resize(count int) error {
	size := int64(count * p.pageSize)
	if p.fileSize == size {
		return nil
	}
	if err := p.unmmap(); err != nil {
		return err
	}
	if err := p.file.Truncate(size); err != nil {
		return err
	}
	p.fileSize = size
	return p.mmap()
}

func (p *OnDisk) mmap() error {
	if p.data != nil {
		_ = p.unmmap()
	}

	d, err := mmap.Map(p.file, p.mmapFlag, 0)
	if err != nil {
		return err
	}
	p.data = d
	return nil
}

func (p *OnDisk) unmmap() error {
	if p.data == nil {
		return nil
	}
	return p.data.Unmap()
}
