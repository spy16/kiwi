package bptree

import (
	"fmt"
	"testing"

	"github.com/spy16/kiwi/io"
)

func TestBPlusTree_Put(t *testing.T) {
	p, err := io.Open(":memory:", 400, false, 0)
	if err != nil {
		t.Fatalf("failed to init pager: %v", err)
	}
	defer p.Close()

	tree, err := New(p, nil)
	if err != nil {
		t.Fatalf("failed to init tree: %v", err)
	}

	for i := 0; i < 400; i++ {
		if err := tree.Put([]byte{byte(65 + i)}, 0); err != nil {
			t.Fatalf("failed: %v", err)
		}
	}

	Print(tree)
	fmt.Println(tree)
}
