// Package blob provides store implementations to store unstrcuctured binary
// data.
package blob

// Store implementations (File, InMem etc.) manage persistence of blobs of
// binary data.
type Store interface {
	// Alloc allocates one page blob and returns.
	Alloc() (*Blob, error)

	// Fetch reads one page with given id and returns a blob handle
	// if a blob entry starts at that page.
	Fetch(id int) (*Blob, error)

	// Free frees all the pages used by record with given id.
	Free(id int) error
}
