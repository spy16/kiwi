package linearhash

import (
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/spy16/kiwi/index"
)

var pageSize = os.Getpagesize()

// Store implements a kiwi storage backend with indexing scheme based on
// on-disk hash table using Lienar Hashing algorithm.
// Refer https://en.wikipedia.org/wiki/Linear_hashing
type Store struct {
	header
	mu *sync.RWMutex

	// index file states
	mf    mmap.MMap
	index *os.File
	size  int64

	// other states
	blobs    BlobStore
	closed   bool
	readOnly bool
}

// Get returns the value associated with the key if found. Returns ErrNotFound
// otherwise.
func (lhs *Store) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, index.ErrEmptyKey
	}

	lhs.mu.RLock()
	defer lhs.mu.RUnlock()

	return nil, nil
}

// Put puts the key and the value offset into the hash index.
func (lhs *Store) Put(key, val []byte) error {
	if lhs.readOnly {
		return index.ErrImmutable
	} else if len(key) == 0 {
		return index.ErrEmptyKey
	}

	lhs.mu.Lock()
	defer lhs.mu.Unlock()

	blob := packKV(key, val)

	offset, err := lhs.blobs.Alloc(blob)
	if err != nil {
		return err
	}

	hash := lhs.hash(key)

	buckets := lhs.bucketCursor(uint32(hash % 1))

	var targetBucket *bucket
	var targetSlot *slot

	err = buckets.ForEach(func(b *bucket) (stop bool, err error) {
		for i := 0; i < int(lhs.slotCount()); i++ {
			sl := b.slot(i)
			if sl.hash == 0 {
				targetBucket = b
				targetSlot = sl
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil && err != io.EOF {
		return err
	}

	if targetBucket != nil {
		targetSlot.hash = hash
		targetSlot.keySz = uint16(len(key))
		targetSlot.valSz = uint32(len(val))
		targetSlot.blobOffset = offset
	}

	return lhs.index.Sync()
}

// Close flushes any remaining writes, closes the file handle and marks
// the store as closed. Subsequent calls to close will not return error.
func (lhs *Store) Close() error {
	lhs.mu.Lock()
	defer lhs.mu.Unlock()

	if lhs.closed {
		return nil
	}

	lhs.closed = true
	_ = lhs.mf.Unlock()
	return lhs.index.Close()
}

func (lhs Store) String() string {
	return fmt.Sprintf(
		"LHStore{name='%s', closed=%t}",
		lhs.index.Name(), lhs.closed,
	)
}

func (lhs *Store) bucketCursor(startBucket uint32) *bucketIterator {
	offset := (startBucket + 1) * lhs.pageSz
	return &bucketIterator{
		head: bucketFrom(lhs.mf[offset:]),
		blob: lhs.blobs,
	}
}

func (lhs *Store) hash(key []byte) uint64 {
	hasher := maphash.Hash{}
	if _, err := hasher.Write(key); err != nil {
		panic(err) // fnv never returns errors
	}
	return hasher.Sum64()
}

func (lhs *Store) init() error {
	if lhs.size > 0 {
		return lhs.readHeader()
	}

	// make 2 pages: 1 for header + 1 for initial bucket
	buf := make([]byte, 2*pageSize)

	// initialize the header with current system page size
	// and other kiwi information
	h := headerFrom(buf)
	h.magic = kiwiMagic
	h.pageSz = uint32(pageSize)
	h.version = kiwiVersion
	h.bucketCount = 1
	h.splitBucket = 0

	// initialize 1 bucket to begin with
	b := bucketFrom(buf[1*os.Getpagesize():])
	b.magic = bucketMagic
	b.overflow = 0

	lhs.header = *h
	_, err := lhs.index.WriteAt(buf, 0)
	return err
}

func (lhs *Store) readHeader() error {
	var buf [0x1000]byte // assume max 4096 page size
	if _, err := lhs.index.ReadAt(buf[:], 0); err != nil {
		return err
	}
	lhs.header = *(headerFrom(buf[:]))
	return lhs.header.validate()
}

func (lhs *Store) mmapFile() error {
	mflag := mmap.RDWR
	if lhs.readOnly {
		mflag = mmap.RDONLY
	}
	mf, err := mmap.Map(lhs.index, mflag, 0)
	if err != nil {
		lhs.Close()
		return err
	}

	lhs.mf = mf
	return mf.Lock()
}
