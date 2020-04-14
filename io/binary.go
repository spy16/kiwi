package io

import (
	"encoding"
	"io"
)

// BinaryWrite marshals and writes the data to the writer.
func BinaryWrite(f io.WriterAt, offset int64, m encoding.BinaryMarshaler) error {
	d, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = f.WriteAt(d, offset)
	return err
}

// BinaryRead reads data from the reader at offset and un-marshals using 'into'.
func BinaryRead(f io.ReaderAt, offset int64, size int, into encoding.BinaryUnmarshaler) error {
	buf := make([]byte, size)
	_, err := f.ReadAt(buf, offset)
	if err != nil {
		return err
	}
	return into.UnmarshalBinary(buf)
}
