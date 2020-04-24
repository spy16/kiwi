package bptree

import "encoding"

// Pager implementations should provide paged storage facilities.
type Pager interface {
	// Alloc should allocate 'n' consecutive pages and return id of
	// the first.
	Alloc(n int) (id int, err error)

	// ReadOnly should return true if the page is in read-only mode.
	ReadOnly() bool

	// Count should return number of pages currently in the paged file.
	Count() int

	// PageSize should return size of one page.
	PageSize() int

	// Marshal should marshal 'from' and write to page.
	Marshal(pageID int, from encoding.BinaryMarshaler) error

	// Unmarshal should read page and unmarshal using 'into'.
	Unmarshal(pageID int, into encoding.BinaryUnmarshaler) error

	// Close should flush any changes and close the underlying file.
	Close() error
}
