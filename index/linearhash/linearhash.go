package linearhash

import (
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

	p, err := io.Open(indexFile, opts.ReadOnly, opts.FileMode)
	if err != nil {
		return nil, err
	}

	idx := &LinearHash{
		mu:       &sync.RWMutex{},
		pager:    p,
		pageSize: p.PageSize(),
		readOnly: opts.ReadOnly,
	}

	// read header if index file is initialized, or initialize if
	// the file is empty.
	if err := idx.open(); err != nil {
		_ = idx.Close()
		return nil, err
	}

	return idx, nil
}

// LinearHash implements on-disk hashing based indexing using Linear Hashing
// algorithm.
type LinearHash struct {
	mu        *sync.RWMutex
	pager     *io.Pager
	readOnly  bool
	pageSize  int
	slotCount int
}

// Close flushes any pending writes and frees the file descriptor.
func (idx *LinearHash) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.pager == nil {
		return nil
	}

	err := idx.pager.Close()
	idx.pager = nil
	return err
}

func (idx *LinearHash) String() string {
	return fmt.Sprintf(
		"LinearHash{pager='%s', closed=%t}",
		idx.pager, idx.pager == nil,
	)
}

func (idx *LinearHash) open() error {
	if idx.pager.Count() == 0 { // empty file, so initialize it
		return idx.init()
	}

	h := header{}
	if err := idx.pager.Unmarshal(0, &h); err != nil {
		return err
	}

	idx.pageSize = int(h.pageSz)
	return h.Validate()
}

func (idx *LinearHash) init() error {
	if idx.isImmutable() {
		return index.ErrImmutable
	}

	return idx.pager.Marshal(0, header{
		magic:   magic,
		pageSz:  uint16(os.Getpagesize()),
		version: version,
	})
}

func (idx *LinearHash) isImmutable() bool {
	return idx.readOnly || idx.pager == nil
}

func (idx *LinearHash) hash(key []byte) uint64 {
	hasher := maphash.Hash{}
	if _, err := hasher.Write(key); err != nil {
		panic(err) // should never return error
	}
	return hasher.Sum64()
}

func (idx *LinearHash) bucketIndex(hash uint64) uint32 {
	return 0
}
