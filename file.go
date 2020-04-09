package kiwi

import (
	"encoding"
	"errors"
	"os"
)

func openFile(path string, readOnly bool, perm os.FileMode) (*os.File, error) {
	flag := os.O_RDWR | os.O_CREATE
	if readOnly {
		flag = os.O_RDONLY
	}
	return os.OpenFile(path, flag, perm)
}

func binaryWrite(f *os.File, offset int64, m encoding.BinaryMarshaler) error {
	d, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = f.WriteAt(d, offset)
	return err
}

func binaryRead(f *os.File, offset int64, size int, into encoding.BinaryUnmarshaler) error {
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		return err
	} else if n < size {
		return errors.New("read insufficient data")
	}
	return into.UnmarshalBinary(buf)
}
