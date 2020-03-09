package blob

import (
	"fmt"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

// OpenFile opens a data file and returns an instance of File. Returns error
// if opening fails or the file has invalid header.
func OpenFile(filePath string, readOnly bool, mode os.FileMode) (*File, error) {
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}

	fh, err := os.OpenFile(filePath, flag, mode)
	if err != nil {
		return nil, err
	}

	fi, err := fh.Stat()
	if err != nil {
		return nil, err
	}

	f := &File{
		mu:   &sync.RWMutex{},
		fh:   fh,
		size: fi.Size(),
	}

	if err := f.init(); err != nil {
		fh.Close()
		return nil, err
	}

	if err := f.mmap(); err != nil {
		fh.Close()
		return nil, err
	}

	return f, nil
}

// File implements a BlobStore using a file.
type File struct {
	dataFileHeader
	mu     *sync.RWMutex
	fh     *os.File
	size   int64
	mf     mmap.MMap
	closed bool
}

// Fetch returns the binary blob starting at the given offset. Returns
// error if offset is invalid or any error occured during reading.
func (f *File) Fetch(offset uint64) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if offset >= uint64(f.size)+blobHeaderSz {
		return nil, fmt.Errorf("invalid offset '%d', file size is %d",
			offset, f.size)
	}

	b := blobFrom(f.mf, offset)
	if b.magic != blobMagic {
		return nil, fmt.Errorf("invalid offset '%d', not beginning of a blob",
			offset)
	}

	return b.getData(), nil
}

// Alloc allocates space required to store the given data, stores it &
// returns the offset where the data was stored.
func (f *File) Alloc(data []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	allocOffset := uint64(f.size)
	dataSz := int64(len(data))
	blobSz := int64(blobHeaderSz) + dataSz

	// TODO: re-use from freelist if possible instead of extending always
	if err := f.extend(blobSz); err != nil {
		return 0, err
	}

	b := blobFrom(f.mf, allocOffset)
	b.magic = blobMagic
	b.flags = blobInUse
	b.size = uint32(dataSz)
	b.setData(data)

	f.count++
	return allocOffset, f.sync()
}

// Free de-allocates/frees the space occupied by the blob stored at given
// offset.
func (f *File) Free(offset uint64) error {
	b, err := f.blobAt(offset)
	if err != nil {
		return err
	}

	b.flags = 0x0 // mark as not in-use
	f.count--

	// TODO: push the offset to a freelist for re-use
	return f.sync()
}

// Close closes the underlying file and marks the store as closed.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true
	_ = f.munmap()
	return f.fh.Close()
}

func (f *File) String() string {
	return fmt.Sprintf("File{name='%s', version=%d, blobs=%d}",
		f.fh.Name(), f.version, f.count)
}

func (f *File) init() error {
	if f.size == 0 {
		// new file, need to be initialized
		buf := make([]byte, headerSize)

		h := headerFrom(buf)
		h.magic = dataFileMagic
		h.version = dataFileVersion
		h.count = 0

		f.dataFileHeader = *h
		f.size = int64(len(buf))
		_, err := f.fh.WriteAt(buf, 0)
		return err
	}

	buf := make([]byte, headerSize)
	if _, err := f.fh.ReadAt(buf[:], 0); err != nil {
		return err
	}
	h := headerFrom(buf[:])
	if err := h.validate(); err != nil {
		return err
	}
	f.dataFileHeader = *h

	return nil
}

func (f *File) sync() error {
	h := headerFrom(f.mf[0:])
	*h = f.dataFileHeader
	return f.fh.Sync()
}

func (f *File) mmap() error {
	mf, err := mmap.Map(f.fh, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	f.mf = mf
	f.dataFileHeader = *(headerFrom(f.mf[0:]))
	return mf.Lock()
}

func (f *File) munmap() error {
	_ = f.mf.Unlock()
	return f.mf.Unmap()
}

func (f *File) extend(bySize int64) error {
	if err := f.munmap(); err != nil {
		return err
	}
	newSize := f.size + bySize
	err := f.fh.Truncate(newSize)
	if err != nil {
		return err
	}
	f.size = newSize
	return f.mmap()
}

func (f *File) blobAt(offset uint64) (*blob, error) {
	if offset >= uint64(f.size)+blobHeaderSz {
		return nil, fmt.Errorf("invalid offset '%d', file size is %d",
			offset, f.size)
	}
	b := blobFrom(f.mf, offset)
	if b.magic != blobMagic {
		return nil, fmt.Errorf("invalid offset '%d', not beginning of a blob",
			offset)
	}
	return b, nil
}
