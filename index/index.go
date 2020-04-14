package index

// Index implementations provide indexing schemes for Kiwi database.
type Index interface {
	Put(entry Entry) error
	Get(key []byte) (*Entry, error)
	Del(key []byte) (*Entry, error)
	Close() error
}

// Entry represents an indexing entry to be stored in Index.
type Entry struct {
	Key      []byte
	BlobID   uint64
	ValueSz  uint64
	Checksum uint32
}
