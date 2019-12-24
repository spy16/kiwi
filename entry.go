package kiwi

import (
	"bytes"
	"sort"
)

type byteSliceList [][]byte

func (bl byteSliceList) search(key []byte) (idx int, found bool) {
	L := len(bl)

	idx = sort.Search(L, func(i int) bool {
		return bytes.Compare(key, bl[i]) != 1
	})
	found = (idx < L) && bytes.Equal(bl[idx], key)

	return idx, found
}

func (bl *byteSliceList) insert(idx int, val []byte) {
	updated := append(*bl, nil)
	copy(updated[idx+1:], updated[idx:])
	updated[idx] = val
	*bl = updated
}

func (bl *byteSliceList) delete(idx int) {
	(*bl)[idx] = nil
	*bl = append((*bl)[:idx], (*bl)[idx+1:]...)
}
