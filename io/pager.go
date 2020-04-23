package io

import (
	"encoding"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
)

const disableMmap = false

// InMemoryFileName can be passed to Open() to create a pager for an ephemeral
// in-memory file.
const InMemoryFileName = ":memory:"

// ErrReadOnly is returned when a write operation is attempted on a read-only
// pager instance.
var ErrReadOnly = errors.New("read-only")

// Open opens the named file and returns a pager instance for it. If the file
// doesn't exist, it will be created if not in read-only mode.
func Open(fileName string, blockSz int, readOnly bool, mode os.FileMode) (*Pager, error) {
	if fileName == InMemoryFileName {
		return newPager(&inMemory{}, blockSz, readOnly, 0)
	}

	if blockSz == 0 {
		blockSz = os.Getpagesize()
	} else if blockSz < 4096 || blockSz%4096 != 0 {
		return nil, errors.New("block size must be multple of 4096")
	}

	mmapFlag := mmap.RDWR
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		mmapFlag = mmap.RDONLY
		flag = os.O_RDONLY
	}

	f, err := os.OpenFile(fileName, flag, mode)
	if err != nil {
		return nil, err
	}

	return newPager(f, blockSz, readOnly, mmapFlag)
}

// newPager creates an instance of pager for given random access file object.
// By default page size is set to the current system page size.
func newPager(file RandomAccessFile, pageSize int, readOnly bool, mmapFlag int) (*Pager, error) {
	size, err := findSize(file)
	if err != nil {
		return nil, err
	}

	osFile, _ := file.(*os.File)

	p := &Pager{
		file:     file,
		fileSize: size,
		readOnly: readOnly,
		pageSize: pageSize,
		osFile:   osFile,
		mmapFlag: mmapFlag,
	}
	p.computeCount()

	if size > 0 {
		if err := p.mmap(); err != nil {
			_ = p.Close()
			return nil, err
		}
	}

	return p, nil
}

// Pager provides facilities for paged I/O on file-like objects with random
// access. If the underlying file is os.File type, memory mapping will be
// enabled when file size is non-zero.
type Pager struct {
	// internal states
	file     RandomAccessFile
	pageSize int
	fileSize int64
	count    int
	readOnly bool

	// memory mapping state for os.File
	osFile   *os.File
	data     mmap.MMap
	mmapFlag int

	// i/o tracking
	writes int
	reads  int
	allocs int
}

// Alloc allocates 'n' new sequential pages and returns the id of the first
// page in sequence.
func (p *Pager) Alloc(n int) (int, error) {
	if p.file == nil {
		return 0, os.ErrClosed
	} else if p.readOnly {
		return 0, ErrReadOnly
	}

	nextID := p.count

	_ = p.unmap()
	targetSize := p.fileSize + int64(n*p.pageSize)
	if err := p.file.Truncate(targetSize); err != nil {
		return 0, err
	}

	p.fileSize = targetSize
	p.computeCount()

	p.allocs++
	return nextID, p.mmap()
}

// Read reads one page of data from the underlying file or mmapped region if
// enabled.
func (p *Pager) Read(id int) ([]byte, error) {
	if id < 0 || id >= p.count {
		return nil, fmt.Errorf("invalid page id (max=%d)", id)
	} else if p.file == nil {
		return nil, os.ErrClosed
	}

	buf := make([]byte, p.pageSize)
	if p.data != nil {
		n := copy(buf, p.data[p.offset(id):])
		if n < p.pageSize {
			return nil, io.EOF
		}
		p.reads++
		return buf, nil
	}

	n, err := p.file.ReadAt(buf, p.offset(id))
	if n < p.pageSize {
		return nil, io.EOF
	}
	p.reads++
	return buf, err
}

// Write writes one page of data to the page with given id. Returns error if
// the data is larger than a page.
func (p *Pager) Write(id int, d []byte) error {
	if id < 0 || id >= p.count {
		return fmt.Errorf("invalid page id=%d (max=%d)", id, p.count-1)
	} else if len(d) > p.pageSize {
		return errors.New("data is larger than a page")
	} else if p.file == nil {
		return os.ErrClosed
	} else if p.readOnly {
		return ErrReadOnly
	}

	if p.data != nil {
		copy(p.data[p.offset(id):], d)
		p.writes++
		return nil
	}

	_, err := p.file.WriteAt(d, p.offset(id))
	if err != nil {
		return err
	}
	p.writes++
	return nil
}

// Marshal writes the marshaled value of 'v' into page with given id.
func (p *Pager) Marshal(id int, v encoding.BinaryMarshaler) error {
	d, err := v.MarshalBinary()
	if err != nil {
		return err
	}
	return p.Write(id, d)
}

// Unmarshal reads the page with given id and unmarshals the page data using
// 'into'.
func (p *Pager) Unmarshal(id int, into encoding.BinaryUnmarshaler) error {
	d, err := p.Read(id)
	if err != nil {
		return err
	}
	return into.UnmarshalBinary(d)
}

// PageSize returns the size of one page used by pager.
func (p *Pager) PageSize() int { return p.pageSize }

// Count returns the number of pages in the underlying file. Returns error if
// the file is closed.
func (p *Pager) Count() int { return p.count }

// ReadOnly returns true if the pager instance is in read-only mode.
func (p *Pager) ReadOnly() bool { return p.readOnly }

// Close closes the underlying file and marks the pager as closed for use.
func (p *Pager) Close() error {
	if p.file == nil {
		return nil
	}
	_ = p.unmap()
	err := p.file.Close()
	p.osFile = nil
	p.file = nil
	return err
}

// Stats returns i/o stats collected by this pager.
func (p *Pager) Stats() Stats {
	return Stats{
		Allocs: p.allocs,
		Reads:  p.reads,
		Writes: p.writes,
	}
}

func (p *Pager) String() string {
	if p.file == nil {
		return fmt.Sprintf("Pager{closed=true}")
	}

	return fmt.Sprintf(
		"Pager{file='%s', readOnly=%t, pageSize=%d, count=%d, mmap=%t}",
		p.file.Name(), p.readOnly, p.pageSize, p.count, p.data != nil,
	)
}

func (p *Pager) computeCount() {
	p.count = int(p.fileSize) / p.pageSize
}

func (p *Pager) offset(id int) int64 {
	return int64(p.pageSize * id)
}

func (p *Pager) mmap() error {
	if disableMmap || p.osFile == nil || p.file == nil || p.fileSize <= 0 {
		return nil
	}

	if err := p.unmap(); err != nil {
		return err
	}

	d, err := mmap.Map(p.osFile, p.mmapFlag, 0)
	if err != nil {
		return err
	}
	p.data = d
	return nil
}

func (p *Pager) unmap() error {
	if p.osFile == nil || p.data == nil {
		return nil
	}
	return p.data.Unmap()
}

// Stats represents I/O statistics collected by the pager.
type Stats struct {
	Writes int
	Reads  int
	Allocs int
}

func (s Stats) String() string {
	return fmt.Sprintf(
		"Stats{writes=%d, allocs=%d, reads=%d}",
		s.Writes, s.Allocs, s.Reads,
	)
}
