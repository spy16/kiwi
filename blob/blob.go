package blob

import (
	"encoding"
)

// Blob is a handle to a blob of binary data in a store.
type Blob struct {
	id   int    // id of the first page in the blob
	size int    // size of the entire blob
	data []byte // data that has been read so far
	file Pager  // underlying paged file
}

// Marshal writes the data obtained using 'from' to the blob.
func (b *Blob) Marshal(from encoding.BinaryMarshaler) error {
	return nil
}

// Unmarshal reads data of the entire blob and unmarshals it using 'into'.
func (b Blob) Unmarshal(into encoding.BinaryUnmarshaler) error {
	return nil
}

func (b *Blob) open(d []byte) error {
	h := blobHeader{}
	if err := h.UnmarshalBinary(d); err != nil {
		return err
	}

	b.size = int(h.totalSize)
	b.data = append(d[blobHeaderSz:h.size])
	return nil
}

func (b *Blob) init() error {
	h := blobHeader{
		flags: 0, // in use
		size:  0, // no data apart from header
		next:  0, // no next pointer
	}

	d, _ := h.MarshalBinary()
	return b.file.Write(b.id, d)
}

func (b *Blob) maxContentSize() int {
	return b.file.PageSize() - blobHeaderSz
}
