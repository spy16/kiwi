package blob

import (
	"errors"
	"os"
)

var _ Pager = (*InMemory)(nil)

// InMemory implements the pager using an in-memory slice.
type InMemory struct {
	pages  [][]byte
	closed bool
}

// PageSize should return the size of one page in.
func (mem *InMemory) PageSize() int {
	return os.Getpagesize()
}

// Alloc should allocate 'n' number of new sequential pages
// and return the id of the first page.
func (mem *InMemory) Alloc(count int) (int, error) {
	id := len(mem.pages)
	for i := 0; i < count; i++ {
		mem.pages = append(mem.pages, make([]byte, mem.PageSize()))
	}
	return id, nil
}

// Fetch should read the page with given id and return the
// data in it.
func (mem *InMemory) Fetch(id int) ([]byte, error) {
	if id >= len(mem.pages) {
		return nil, errors.New("non-existent page")
	}
	buf := make([]byte, mem.PageSize())
	copy(buf, mem.pages[id])
	return buf, nil
}

// Write should write the changes to the page with given id.
func (mem *InMemory) Write(id int, d []byte) error {
	if id >= len(mem.pages) {
		return errors.New("non-existent page")
	} else if len(d) > mem.PageSize() {
		return errors.New("data is larger than page")
	}

	copy(mem.pages[id], d)
	return nil
}

// Close closes the pager.
func (mem *InMemory) Close() error {
	if mem.closed {
		return nil
	}

	mem.closed = true
	mem.pages = nil
	return nil
}
