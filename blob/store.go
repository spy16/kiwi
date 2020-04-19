package blob

import (
	"io"
	"sync"
)

// New initializes an instance of blob store using given pager.
func New(pager Pager) *Store {
	return &Store{
		mu:    &sync.RWMutex{},
		pager: pager,
	}
}

// Store provides functions for storing blobs of binary data in files.
type Store struct {
	mu       *sync.RWMutex
	pager    Pager
	freeList []int
}

// Alloc allocates a new blob record in the storage and returns a handle
// to access the record.
func (store *Store) Alloc() (*Blob, error) {
	// TODO: alloc using free-list
	id, err := store.pager.Alloc(1)
	if err != nil {
		return nil, err
	}

	b := &Blob{
		id:   id,
		file: store.pager,
	}

	if err := b.init(); err != nil {
		store.freePage(id)
		return nil, err
	}

	return b, nil
}

// Fetch fetches a blob handle to the blob with given identifier which can
// be used to read/write the data from/to blob.
func (store *Store) Fetch(id int) (*Blob, error) {
	p, err := store.pager.Fetch(id)
	if err != nil {
		return nil, err
	}

	b := &Blob{
		id:   id,
		file: store.pager,
	}

	if err := b.open(p); err != nil {
		return nil, err
	}

	return b, nil
}

// Free marks the blob with given id as freed. All the pages used by the
// blob are freed and sent to free-list.
func (store *Store) Free(id int) error {
	// TODO: read the blob and all it's pages and free them.
	return nil
}

// Close writes the freelist to the underlying file and closes it.
func (store *Store) Close() (err error) {
	if store.pager == nil {
		return nil // already closed
	}

	// TODO: write the freelist

	// close the pager if it supports it
	if closer, ok := store.pager.(io.Closer); ok {
		err = closer.Close()
	}
	return err
}

func (store *Store) freePage(ids ...int) {
	store.freeList = append(store.freeList, ids...)
}
