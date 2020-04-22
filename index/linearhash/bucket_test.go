package linearhash

import (
	"reflect"
	"testing"
)

func Test_bucket_binary(t *testing.T) {
	original := bucket{
		flags:    0xFF,
		id:       123,
		overflow: 456,
		ptr:      789,
	}

	d, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() unexpected error: %#v", err)
	}

	got := bucket{}
	if err := got.UnmarshalBinary(d); err != nil {
		t.Errorf("UnmarshalBinary() unexpected error: %#v", err)
	}

	if !reflect.DeepEqual(original, got) {
		t.Errorf("want=%#v\ngot=%#v", original, got)
	}
}
