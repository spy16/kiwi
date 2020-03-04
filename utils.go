package kiwi

// KVBytes is a convenience function to convert a string key value
// pair to a byte-slice key-value pair.
func KVBytes(k, v string) (key, val []byte) {
	return []byte(k), []byte(v)
}
