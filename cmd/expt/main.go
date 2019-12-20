package main

import (
	"bytes"
	"fmt"

	"github.com/spy16/kiwi/bptree"
)

func main() {
	// testBinarySearch()
	tree := bptree.New(3)
	tree.Put([]byte("A"), nil) // 65
	bptree.Print(tree)
	tree.Put([]byte("F"), nil) // 70
	bptree.Print(tree)
	tree.Put([]byte("D"), nil) // 68
	bptree.Print(tree)
	tree.Put([]byte("B"), nil) // 66
	bptree.Print(tree)
	tree.Put([]byte("C"), nil) // 67
	bptree.Print(tree)
	tree.Put([]byte("E"), nil) // 69
	bptree.Print(tree)
}

func testBinarySearch() {
	// arr := [][]byte{[]byte("A"), []byte("B"), []byte("D"), []byte("E"), []byte("F")}
	arr := [][]byte{[]byte("A")}
	idx, found := binarySearch(arr, []byte("F"))
	fmt.Println(idx, found)

	arr = append(arr, nil)
	copy(arr[idx+1:], arr[idx:])
	arr[idx] = []byte("F")
	fmt.Println(arr)
}

func binarySearch(arr [][]byte, key []byte) (idx int, found bool) {
	lo, hi := 0, len(arr)-1
	mid := 0

	for lo <= hi {
		mid = (lo + hi) / 2

		switch bytes.Compare(arr[mid], key) {
		case 0:
			return mid, true

		case -1:
			lo = mid + 1

		case 1:
			hi = mid - 1
		}
	}

	return lo, false
}
