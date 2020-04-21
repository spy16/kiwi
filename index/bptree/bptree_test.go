package bptree

import "testing"

func TestBPlusTree_Put(t *testing.T) {
	tree, err := Open(":memory:", false, 0)
	if err != nil {
		t.Fatalf("failed to create in-memory tree: %#v", err)
	}
	defer tree.Close()

	if tree.size != 0 {
		t.Errorf("expected tree size to be 0, not %d", tree.size)
	}

	insertKeys := 60

	for i := 0; i < insertKeys; i++ {
		if err := tree.Put([]byte{byte(i)}, uint64(i)); err != nil {
			t.Errorf("Put() unexpected error: %#v", err)
		}
	}

	if int(tree.size) != insertKeys {
		t.Errorf("expected tree size to be %d, not %d", insertKeys, tree.size)
	}

	for i := 0; i < insertKeys; i++ {
		key := []byte{byte(i)}
		v, err := tree.Get(key)
		if err != nil {
			t.Fatalf("Get('%#x') unexpected error: %#v", key, err)
		}

		if v != uint64(i) {
			t.Errorf("Get() expected value to be %d, not %d", uint64(i), v)
		}
	}
}
