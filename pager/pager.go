package pager

import "errors"

// Open opens the file and initializes a pager instance for it. If 'opts'
// is nil, defaultOptions are used.
func Open(filePath string, opts *Options) (Pager, error) {
	if opts == nil {
		opts = &defaultOptions
	}
	opts.init()

	if filePath == ":memory:" {
		return nil, errors.New("not implemented")
	}

	return openOnDisk(filePath, *opts)
}

// Pager provides a strictly paged I/O access to file-like objects.
type Pager interface {
	Count() int
	Alloc(count int) error
	Read(id int) ([]byte, error)
	Write(id int, d []byte) error
}
