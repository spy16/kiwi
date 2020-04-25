package bptree

import (
	"reflect"
	"testing"
)

func Test_metadata_Binary(t *testing.T) {
	original := metadata{
		magic:    0xD0D0,
		version:  0x1,
		flags:    0xFD,
		maxKeySz: 100,
		pageSz:   4096,
		rootID:   10,
		size:     1000,
		freeList: []int{},
	}

	d, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() unexpected error: %#v", err)
	}

	got := metadata{}
	if err := got.UnmarshalBinary(d); err != nil {
		t.Fatalf("UnmarshalBinary() unexpected error: %#v", err)
	}

	if !reflect.DeepEqual(original, got) {
		t.Errorf("want=%#v\ngot=%#v", original, got)
	}
}
