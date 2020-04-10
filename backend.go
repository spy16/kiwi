package kiwi

// Backend represents the storage backend for the Kiwi database.
type Backend interface {
	Get(key []byte) ([]byte, error)
	Put(key, val []byte) error
	Del(key []byte) error
	Close() error
}

// BackendType indicates the storage backend to be used.
type BackendType uint16

func (b BackendType) String() string {
	switch b {
	default:
		return "unknown"
	}
}

// inMemory implements Kiwi storage backend using Go native map.
type inMemory struct {
	data map[string][]byte
}

func (mem *inMemory) Get(key []byte) ([]byte, error) {
	v, found := mem.data[string(key)]
	if !found {
		return nil, ErrNotFound
	}
	return v, nil
}

func (mem *inMemory) Put(key []byte, val []byte) error {
	if mem.data == nil {
		mem.data = map[string][]byte{}
	}
	mem.data[string(key)] = val
	return nil
}

func (mem *inMemory) Del(key []byte) error {
	delete(mem.data, string(key))
	return nil
}

func (mem *inMemory) Close() error { return nil }
