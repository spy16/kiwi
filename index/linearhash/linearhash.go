package linearhash

import (
	"fmt"
	"hash/maphash"
	"os"
	"sync"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/io"
)

// Open opens the file as linear-hash indexing file and returns the indexer
// instance. If 'opts' is nil, uses default options.
func Open(indexFile string, opts *Options) (*LinearHash, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	p, err := io.Open(indexFile, os.Getpagesize(), opts.ReadOnly, opts.FileMode)
	if err != nil {
		return nil, err
	}

	idx := &LinearHash{
		mu:       &sync.RWMutex{},
		pager:    p,
		pageSize: p.PageSize(),
		readOnly: p.ReadOnly(),
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

// Get finds the index entry for given key in the hash table and returns. If
// not entry found, returns ErrKeyNotFound.
func (idx *LinearHash) Get(key []byte) (uint64, error) {
	hash := idx.hash(key)

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.getEntry(key, hash)
}

// Put inserts the indexing entry into the hash table.
func (idx *LinearHash) Put(key []byte, val uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.isImmutable() {
		return index.ErrImmutable
	}

	return idx.putEntry(entry{key: key, val: val})
}

// Del removes the entry for the given key from the hash table and returns
// the removed entry.
func (idx *LinearHash) Del(key []byte) (uint64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.isImmutable() {
		return 0, index.ErrImmutable
	}

	return 0, index.ErrKeyNotFound
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

func (idx *LinearHash) getEntry(key []byte, hash uint64) (uint64, error) {
	return 0, nil
}

func (idx *LinearHash) putEntry(e entry) error {
	return nil
}

func (idx *LinearHash) locateSlot(key []byte) (res *bucket, slotID int, err error) {
	hash := idx.hash(key)
	bucketID := idx.bucketIndex(hash)

	bucketPage, err := idx.pager.Read(int(bucketID))
	if err != nil {
		return nil, 0, err
	}

	b := &bucket{}
	if err := b.UnmarshalBinary(bucketPage); err != nil {
		return nil, 0, err
	}

	for b != nil {
		for i := 0; i < int(idx.slotCount); i++ {
			sl := b.slot(i)
			if sl.hash == 0 { // an empty slot
				return b, i, nil
			} else if sl.hash == hash {
				return b, i, nil
			}
		}

		b, err = b.next(idx) // follow the bucket overflow pointer
		if err != nil {
			return nil, 0, err
		}
	}

	return nil, 0, nil
}

func (idx *LinearHash) open() error {
	if idx.pager.Count() == 0 {
		// empty file, so initialize it
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

	_, err := idx.pager.Alloc(1)
	if err != nil {
		return err
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

type entry struct {
	key []byte
	val uint64
}
