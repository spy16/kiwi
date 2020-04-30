package io

import "errors"

var _ BlockFile = (*InMem)(nil)

// InMem implements an ephemeral BlockFile using in-memory byte slice.
// This implementation of BlockFile is meant for testing only.
type InMem struct {
	blockSz  int
	readOnly bool
	closed   bool
	data     []byte
}

// Slice returns a slice of the memory mapped region starting at the block
// with the given id. Incorrect handling of the returned slice can cause
// segfaults or unexpected behavior. Any Alloc() calls will invalidate the
// returned slice.
func (mem *InMem) Slice(id int) ([]byte, error) {
	offset := id * mem.blockSz
	if id < 0 || offset >= len(mem.data) {
		return nil, errors.New("non-existent block")
	}
	return mem.data[offset:], nil
}

// Alloc allocates n new sequential blocks and returns the id of the first.
func (mem *InMem) Alloc(n int) (int, []byte, error) {
	size := mem.blockSz * n
	id := len(mem.data) / mem.blockSz
	mem.data = append(mem.data, make([]byte, size)...)

	sl, err := mem.Slice(id)
	return id, sl, err
}

// Info returns information about the block file state/configuration.
func (mem *InMem) Info() (name string, count, blockSz int, readOnly bool) {
	return ":memory:", len(mem.data) / mem.blockSz, mem.blockSz, mem.readOnly
}

// Close flushes any pending writes and closes the file.
func (mem *InMem) Close() error {
	if mem.closed {
		return nil
	}
	mem.data = nil
	mem.closed = true
	return nil
}
