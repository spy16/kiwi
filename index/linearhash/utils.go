package linearhash

import "os"

func openFile(file string, mode os.FileMode, readOnly bool) (*os.File, int64, error) {
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}

	fh, err := os.OpenFile(file, flag, mode)
	if err != nil {
		return nil, 0, err
	}

	fi, err := fh.Stat()
	if err != nil {
		fh.Close()
		return nil, 0, err
	}

	return fh, fi.Size(), nil
}

func packKV(k, v []byte) []byte {
	d := make([]byte, len(k)+len(v))
	copy(d[0:], k)
	copy(d[len(k):], v)
	return d
}

func unpackKV(blob []byte, keySz int) (k, v []byte) {
	k = make([]byte, keySz)
	v = make([]byte, len(blob)-keySz)

	copy(k, blob[:keySz])
	copy(v, blob[keySz:])
	return k, v
}
