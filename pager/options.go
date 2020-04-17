package pager

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

var defaultOptions = Options{
	Magic:    uint32(0xABCDEF00),
	FileMode: os.ModePerm,
	PageSize: os.Getpagesize(),
	ReadOnly: false,
}

// Options represents configuration options for pager.
type Options struct {
	Magic    uint32
	ReadOnly bool
	FileMode os.FileMode
	PageSize int

	fileFlag int
	mmapFlag int
}

func (opts *Options) init() {
	opts.mmapFlag = mmap.RDWR
	opts.fileFlag = os.O_CREATE | os.O_RDWR
	if opts.ReadOnly {
		opts.fileFlag = os.O_RDONLY
		opts.mmapFlag = mmap.RDONLY
	}
}
