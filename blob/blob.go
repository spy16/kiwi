// Package blob provides store implementations to store unstrcuctured binary
// data.
package blob

// Store implementations (File, InMem etc.) manage persistence of blobs of
// binary data.
type Store interface {
	// Alloc allocates pages and stores the data into them.
	Alloc(data []byte) (id int, err error)

	// Fetch reads an entire record (multiple pages) and returns
	// the read data.
	Fetch(id int) ([]byte, error)

	// Free frees all the pages used by record with given id.
	Free(id int) error
}
