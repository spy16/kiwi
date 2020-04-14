package linearhash

import "os"

var defaultOptions = Options{
	ReadOnly: false,
	FileMode: os.ModePerm,
}

// Options can be provided to Open() to configure initialization.
type Options struct {
	ReadOnly bool
	FileMode os.FileMode
}
