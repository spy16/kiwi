package blob

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var _ Pager = (*OnDisk)(nil)

const (
	version  = 0x1
	headerSz = int(unsafe.Sizeof(header{}))
)

// bin is the byte order used for marshalling any data to file.
var bin = binary.LittleEndian

var defaultOptions = Options{
	Magic:    0xB10B,
	ReadOnly: false,
	FileMode: os.ModePerm,
}

// OpenOnDisk opens the given file as a blob store and returns an OnDisk
// instance to manage blobs.
func OpenOnDisk(fileName string, opts *Options) (*OnDisk, error) {
	if opts == nil {
		opts = &defaultOptions
	}
	opts.init()

	f, err := os.OpenFile(fileName, opts.fileFlag, opts.FileMode)
	if err != nil {
		return nil, err
	}

	pf := &OnDisk{
		file:     f,
		magic:    opts.Magic,
		mmapFlag: opts.mmapFlag,
	}

	if err := pf.open(); err != nil {
		_ = f.Close()
		return nil, err
	}

	if err := pf.mmap(); err != nil {
		_ = pf.Close()
		return nil, err
	}

	return pf, nil
}

// OnDisk represents an on-disk store which can store arbitrary blobs
// of bianry data.
type OnDisk struct {
	magic uint16 // custom magic marker

	// file states
	file     *os.File  // underlying file
	data     mmap.MMap // mmapped memory region
	size     int64     // file size tracker
	mmapFlag int       // flags for memory mapping

	// paging states
	pageSize  int // size of one page
	pageCount int // number of pages in file
}

// Alloc allocates 'n' new pages by extending the file and returns the
// id of the first page from the newly added sequence of pages.
func (pf *OnDisk) Alloc(n int) (int, error) {
	nextID := pf.pageCount
	size := int64(n*pf.pageSize) + pf.size
	if err := pf.file.Truncate(size); err != nil {
		return 0, err
	}
	pf.pageCount += n
	return nextID, nil
}

// Fetch reads one page with given identifier. Returns error if a page
// with given id doesn't exist.
func (pf *OnDisk) Fetch(id int) ([]byte, error) {
	if id < 0 || id >= pf.pageCount {
		return nil, errors.New("write: non-existent page")
	}

	buf := make([]byte, pf.pageSize)
	offset := int64(id * pf.pageSize)
	return buf, pf.readAt(buf, offset)
}

// Write writes the given data slice into the page with given identifier.
// Returns error if the data is bigger than the page or if the id is not
// valid.
func (pf *OnDisk) Write(id int, d []byte) error {
	if id < 0 || id >= pf.pageCount {
		return errors.New("write: non-existent page")
	} else if len(d) > pf.pageSize {
		return errors.New("data is larger than page")
	}

	offset := int64(id * pf.pageSize)
	return pf.writeAt(d, offset)
}

// Count returns the number of pages in the underlying file.
func (pf *OnDisk) Count() int { return pf.pageCount }

// PageSize returns the size of one page managed by this pager.
func (pf *OnDisk) PageSize() int { return pf.pageSize }

// Close flushes any pending writes and closes the underlying file.
func (pf *OnDisk) Close() error {
	if pf.file == nil {
		return nil
	}
	_ = pf.unmap()
	err := pf.file.Close()
	pf.file = nil
	return err
}

func (pf *OnDisk) String() string {
	return fmt.Sprintf(
		"OnDisk{file='%s', pageSize=%d}",
		pf.file.Name(), pf.pageSize,
	)
}

func (pf *OnDisk) readAt(buf []byte, offset int64) error {
	if pf.data == nil {
		_, err := pf.file.ReadAt(buf, offset)
		return err
	}
	copy(buf, pf.data[offset:])
	return nil
}

func (pf *OnDisk) writeAt(buf []byte, offset int64) error {
	if pf.data == nil {
		_, err := pf.file.WriteAt(buf, offset)
		return err
	}
	copy(pf.data[offset:], buf)
	return nil
}

func (pf *OnDisk) open() error {
	fi, err := pf.file.Stat()
	if err != nil {
		return err
	}
	pf.size = fi.Size()

	// assume system page size for now
	pf.setPageSize(os.Getpagesize())

	if pf.size == 0 {
		// empty file, need to initialize
		return pf.writeHeader(header{
			magic:    pf.magic,
			version:  version,
			flags:    0,
			pageSize: uint16(pf.pageSize),
		})
	}

	return pf.readHeader()
}

func (pf *OnDisk) readHeader() error {
	d, err := pf.Fetch(0)
	if err != nil {
		return err
	}
	h := &header{}
	if err := h.UnmarshalBinary(d); err != nil {
		return err
	}
	pf.setPageSize(int(h.pageSize))
	return nil
}

func (pf *OnDisk) writeHeader(h header) error {
	if pf.pageCount <= 0 {
		if _, err := pf.Alloc(1); err != nil {
			return err
		}
	}
	d, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	return pf.Write(0, d)
}

func (pf *OnDisk) setPageSize(pageSz int) {
	pf.pageSize = pageSz
	pf.pageCount = int(pf.size) / pageSz
}

func (pf *OnDisk) mmap() error {
	if pf.data != nil {
		_ = pf.unmap()
	}

	d, err := mmap.Map(pf.file, pf.mmapFlag, 0)
	if err != nil {
		return err
	}
	pf.data = d
	return nil
}

func (pf *OnDisk) unmap() error {
	if pf.data == nil {
		return nil
	}
	err := pf.data.Unmap()
	pf.data = nil
	return err
}

type header struct {
	magic    uint16 // custom marker
	version  uint8  // version of the blob store
	flags    uint8  // control flags (not used)
	pageSize uint16 // pageSize the file was initialized with.
}

func (h header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSz)

	bin.PutUint16(buf[0:2], h.magic)
	buf[2] = h.version
	buf[3] = h.flags
	bin.PutUint16(buf[4:6], h.pageSize)

	return buf, nil
}

func (h *header) UnmarshalBinary(d []byte) error {
	if len(d) < headerSz {
		return errors.New("in-sufficient data")
	} else if h == nil {
		return errors.New("cannot unmarshal into nil header")
	}

	h.magic = bin.Uint16(d[0:2])
	h.version = d[2]
	h.flags = d[3]
	h.pageSize = bin.Uint16(d[4:6])

	return nil
}

// Options provides configuration options for an OnDisk pager.
type Options struct {
	Magic    uint16
	ReadOnly bool
	FileMode os.FileMode

	fileFlag int
	mmapFlag int
}

func (opts *Options) init() {
	opts.fileFlag = os.O_CREATE | os.O_RDWR
	opts.mmapFlag = mmap.RDWR

	if opts.ReadOnly {
		opts.fileFlag = os.O_RDONLY
		opts.mmapFlag = mmap.RDONLY
	}
}
