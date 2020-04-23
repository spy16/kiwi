package bplustree

import (
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	tree := &BPlusTree{
		degree:     4,
		leafDegree: 3,
		nextID:     1,
		nodes:      map[int]*node{},
		size:       0,
	}

	for i := 0; i < 21; i++ {
		if err := tree.Put([]byte{byte(65 + i)}, 0); err != nil {
			t.Fatalf("failed: %v", err)
		}
	}

	Print(tree)
}
