package main

import (
	"fmt"

	"github.com/spy16/kiwi/bptree"
)

func main() {
	// testBinarySearch()
	tree := bptree.New(3)
	tree.Put([]byte("A"), nil)             // 65
	tree.Put([]byte("F"), nil)             // 70
	tree.Put([]byte("D"), nil)             // 68
	tree.Put([]byte("B"), nil)             // 66
	tree.Put([]byte("C"), nil)             // 67
	tree.Put([]byte("E"), nil)             // 69
	tree.Put([]byte("G"), nil)             // 71
	tree.Put([]byte("H"), nil)             // 72
	tree.Put([]byte("I"), []byte("hello")) // 73
	tree.Put([]byte("J"), nil)             // 74
	bptree.Print(tree)

	fmt.Println(tree.Get([]byte("Z")))
}
