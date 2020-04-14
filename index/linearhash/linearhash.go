package linearhash

import (
	"encoding"
	"fmt"
	"hash/maphash"
	"os"
	"sync"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/io"
)

var _ index.Index = (*LinearHash)(nil)

// Open opens the file as linear-hash indexing file and returns the
// indexer instance. If 'opts' is nil, uses default options.
func Open(indexFile string, opts *Options) (*LinearHash, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	flag := os.O_CREATE | os.O_RDWR
	if opts.ReadOnly {
		flag = os.O_RDONLY
	}

	fh, err := io.OpenFile(indexFile, flag, opts.FileMode)
	if err != nil {
		return nil, err
	}

	idx := &LinearHash{
		mu:       &sync.RWMutex{},
		file:     fh,
		readOnly: opts.ReadOnly,
	}

	// read header if index file is initialized, or initialize if
	// the file is empty.
	if err := idx.open(); err != nil {
		_ = idx.Close()
		return nil, err
	}

	// memory map the entire file and lock the file to prevent swap
	if err := idx.file.MMap(flag, true); err != nil {
		_ = idx.Close()
		return nil, err
	}

	return idx, nil
}

// LinearHash implements on-disk hashing based indexing using Linear Hashing
// algorithm.
type LinearHash struct {
	mu        *sync.RWMutex
	file      io.File
	readOnly  bool
	pageSize  int
	slotCount int
}

// Close flushes any pending writes and frees the file descriptor.
func (idx *LinearHash) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return nil
	}

	err := idx.file.Close()
	idx.file = nil
	return err
}

func (idx *LinearHash) String() string {
	return fmt.Sprintf(
		"LinearHash{name='%s', closed=%t}",
		idx.file.Name(), idx.file == nil,
	)
}

func (idx *LinearHash) open() error {
	size, err := idx.file.Size()
	if err != nil {
		return err
	}

	if size == 0 { // empty file, so initialize it
		return idx.init()
	}

	// open existing index file and read the header from first
	// page.
	h := header{}
	if err := io.BinaryRead(idx.file, 0, os.Getpagesize(), &h); err != nil {
		return err
	}
	if err := h.Validate(); err != nil {
		return err
	}

	idx.pageSize = int(h.pageSz)
	return nil
}

func (idx *LinearHash) init() error {
	if idx.isImmutable() {
		return index.ErrImmutable
	}

	if err := idx.file.Truncate(int64(os.Getpagesize())); err != nil {
		return err
	}

	h := header{
		magic:   magic,
		pageSz:  uint16(os.Getpagesize()),
		version: version,
	}

	return io.BinaryWrite(idx.file, 0, h)
}

func (idx *LinearHash) isImmutable() bool {
	return idx.readOnly || idx.file == nil
}

func (idx *LinearHash) hash(key []byte) uint64 {
	hasher := maphash.Hash{}
	if _, err := hasher.Write(key); err != nil {
		panic(err) // should never return error
	}
	return hasher.Sum64()
}

// pageOffset returns the offset in file for the given page index.
// Always skips the first page since it's reserved for header.
func (idx *LinearHash) pageOffset(id uint32) int64 {
	return int64((id + 1) * uint32(idx.pageSize))
}

// readPage reads exactly one page of data starting at offset for given
// page index from file and unmarshals.
func (idx *LinearHash) readPage(id uint32, into encoding.BinaryUnmarshaler) error {
	return io.BinaryRead(idx.file, idx.pageOffset(id), idx.pageSize, into)
}

// writePage marshals and writes data starting at offset for given page
// index. Page size constraint is not enforced here.
func (idx *LinearHash) writePage(id uint32, from encoding.BinaryMarshaler) error {
	return io.BinaryWrite(idx.file, idx.pageOffset(id), from)
}

func (idx *LinearHash) bucketIndex(hash uint64) uint32 {
	return 0
}
