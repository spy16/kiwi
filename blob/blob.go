// Package blob provides store implementations to store unstrcuctured binary
// data.
package blob

// Store implementations (File, InMem etc.) manage persistence of blobs of
// binary data.
type Store interface {
	// Fetch returns the binary blob starting at the given offset. Returns
	// error if offset is invalid or any error occured during reading.
	Fetch(offset uint64) ([]byte, error)

	// Alloc allocates space required to store the given data, stores it &
	// returns the offset where the data was stored.
	Alloc(data []byte) (offset uint64, err error)

	// Free de-allocates/frees the space occupied by the blob stored at given
	// offset. After free, same offset may be re-used by Alloc()
	Free(offset uint64) error
}
