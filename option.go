package kiwi

import (
	"log"
	"os"
)

var defaultOptions = Options{
	Backend:  0,
	FileMode: os.ModePerm,
	ReadOnly: false,
	Log: func(msg string, args ...interface{}) {
		log.Printf(msg, args...)
	},
}

// Options represents the initialization options for Kiwi database.
type Options struct {
	// Options applicable only during creation of DB.
	Backend  BackendType
	FileMode os.FileMode

	ReadOnly bool
	Log      func(msg string, args ...interface{})
}
