package index

import (
	"hash/crc32"
)

// PackKV returns a slice with key-value packed together.
func PackKV(k, v []byte) []byte {
	d := make([]byte, len(k)+len(v))
	copy(d[0:], k)
	copy(d[len(k):], v)
	return d
}

// UnpackKV unpacks a blob of binary data into key and value.
func UnpackKV(blob []byte, keySz int) (k, v []byte) {
	k = make([]byte, keySz)
	v = make([]byte, len(blob)-keySz)

	copy(k, blob[:keySz])
	copy(v, blob[keySz:])
	return k, v
}

// Checksum returns CRC32 checksum for given data.
func Checksum(d []byte) uint32 {
	return crc32.Checksum(d, crc32.IEEETable)
}
