package io

import (
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

var _ File = (*OnDisk)(nil)

// OnDisk is a wrapper around os.File and provides additional functions for
// dealing with files.
type OnDisk struct {
	mu     *sync.RWMutex
	fh     *os.File
	locked bool
	data   mmap.MMap
}

// ReadAt uses ReadAt method of os.File in case this file is not memory
// mapped. Otherwise uses the memory-mapped region and reads from given
// offset.
func (f *OnDisk) ReadAt(b []byte, offset int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.data == nil {
		return f.fh.ReadAt(b, offset)
	}
	return copy(b, f.data[offset:]), nil
}

// WriteAt uses WriteAt method of os.File in case this file is not memory
// mapped. Otherwise uses the memory-mapped region and writes from given
// offset.
func (f *OnDisk) WriteAt(b []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.data == nil {
		return f.fh.WriteAt(b, offset)
	}
	return copy(f.data[offset:], b), nil
}

// Truncate truncates the file to given size. Truncate frees the memory
// mapping before resize.
func (f *OnDisk) Truncate(size int64) error {
	_ = f.MUnmap()
	return f.fh.Truncate(size)
}

// Size returns the size of the file.
func (f *OnDisk) Size() (int64, error) {
	stat, err := f.fh.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// Close flushes any pending write in case of memory mapped file, and
// closes the underlying file handle.
func (f *OnDisk) Close() error {
	_ = f.MUnmap()
	return f.fh.Close()
}

// Name returns the name of the open file.
func (f *OnDisk) Name() string {
	return f.fh.Name()
}

// MMap memory maps the file to an internal bytes buffer.
func (f *OnDisk) MMap(flag int, lock bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	m, err := mmap.Map(f.fh, flag, 0)
	if err != nil {
		return err
	}
	f.data = m
	if lock {
		_ = f.data.Lock()
	}
	return nil
}

// MUnmap flushes any pending writes and frees the memory mapped region.
func (f *OnDisk) MUnmap() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.locked {
		_ = f.data.Unlock()
	}
	err := f.data.Unmap()
	f.data = nil
	return err
}
