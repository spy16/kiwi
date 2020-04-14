package linearhash

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
