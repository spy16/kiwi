package bptree

import (
	"reflect"
	"testing"
)

func Test_node_Search(t *testing.T) {
	n := node{
		entries: []entry{
			{key: []byte("A")},
			{key: []byte("B")},
			{key: []byte("C")},
			{key: []byte("D")},
			{key: []byte("E")},
			{key: []byte("F")},
			{key: []byte("G")},
		},
	}

	idx, found := n.search([]byte("D"))
	assert(t, found, "expected key to exist")
	assert(t, idx == 3, "expected index to be 3 not %d", idx)

	idx, found = n.search([]byte("A"))
	assert(t, found, "expected key to exist")
	assert(t, idx == 0, "expected index to be 0 not %d", idx)

	idx, found = n.search([]byte("G"))
	assert(t, found, "expected key to exist")
	assert(t, idx == 6, "expected index to be 6 not %d", idx)

	idx, found = n.search([]byte("X"))
	assert(t, !found, "expected key to not exist")
	assert(t, idx == 7, "expected insertion index to be 7 not %d", idx)
}

func Test_node_Leaf_Binary(t *testing.T) {
	original := node{
		id: 10,
		entries: []entry{
			{key: []byte("hello"), val: 10},
			{key: []byte("world"), val: 100},
		},
		next: 13,
		prev: 10,
	}

	d, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal: %#v", err)
	}
	original.id = 0

	got := node{}
	if err := got.UnmarshalBinary(d); err != nil {
		t.Fatalf("failed to unmarshal: %#v", err)
	}

	if !reflect.DeepEqual(original, got) {
		t.Errorf("want=%#v\ngot=%#v", original, got)
	}
}

func Test_node_Internal_Binary(t *testing.T) {
	original := node{
		id: 10,
		entries: []entry{
			{key: []byte("hello")},
			{key: []byte("world")},
		},
		children: []int{3, 18, 4},
	}

	d, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal: %#v", err)
	}
	original.id = 0

	got := node{}
	if err := got.UnmarshalBinary(d); err != nil {
		t.Fatalf("failed to unmarshal: %#v", err)
	}

	if !reflect.DeepEqual(original, got) {
		t.Errorf("want=%#v\ngot=%#v", original, got)
	}
}

func assert(t *testing.T, cond bool, msg string, args ...interface{}) {
	if cond {
		return
	}
	t.Errorf(msg, args...)
}
