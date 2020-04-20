package blob

import "io"

// Pager represents a file-like object with strict page based I/O access.
type Pager interface {
	io.Closer

	// Count should return the number of pages in the pager.
	Count() int

	// PageSize should return the size of one page in.
	PageSize() int

	// Alloc should allocate 'n' number of new sequential pages
	// and return the id of the first page.
	Alloc(count int) (int, error)

	// Fetch should read the page with given id and return the
	// data in it.
	Fetch(id int) ([]byte, error)

	// Write should write the changes to the page with given id.
	Write(id int, d []byte) error
}
