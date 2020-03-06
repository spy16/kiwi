package kiwi_test

import (
	"reflect"
	"testing"

	"github.com/spy16/kiwi"
)

func TestInMemBlobStore_Alloc(t *testing.T) {
	data := []byte{0, 1, 3, 5}
	inmem := &kiwi.InMemBlobStore{}

	offset, err := inmem.Alloc(data)
	if err != nil {
		t.Errorf("Alloc() unexpected error: %+v", err)
	}

	if offset != 0 {
		t.Errorf("Alloc() expected first alloc to return offset 0, got %d", offset)
	}

	offset, err = inmem.Alloc(data)
	if err != nil {
		t.Errorf("Alloc() unexpected error: %+v", err)
	}

	if offset != 1 {
		t.Errorf("Alloc() expected second alloc to return offset 1, got %d", offset)
	}
}

func TestInMemBlobStore_Fetch(t *testing.T) {
	data := []byte{0, 1, 3, 5}
	inmem := &kiwi.InMemBlobStore{}
	offset, err := inmem.Alloc(data)
	if err != nil {
		t.Fatalf("Alloc() unexpected error: %+v", err)
	}

	t.Run("ValidOffset", func(t *testing.T) {
		got, err := inmem.Fetch(offset)
		if err != nil {
			t.Errorf("Fetch() unexpected error: %+v", err)
		}

		if !reflect.DeepEqual(data, got) {
			t.Errorf("Fetch() want=%+v, got=%+v", data, got)
		}
	})

	t.Run("InvalidOffset", func(t *testing.T) {
		_, err := inmem.Fetch(1009898)
		if err == nil {
			t.Errorf("Fetch() expecting error, got nil")
		}
	})
}

func TestInMemBlobStore_Free(t *testing.T) {
	data := []byte{0, 1, 3, 5}
	inmem := &kiwi.InMemBlobStore{}
	firstAllocOffset, err := inmem.Alloc(data)
	if err != nil {
		t.Fatalf("Alloc() unexpected error: %+v", err)
	}

	// Free valid offset.
	if err := inmem.Free(firstAllocOffset); err != nil {
		t.Errorf("Free() unexpected error: %+v", err)
	}

	// Free already freed offset.
	if err := inmem.Free(firstAllocOffset); err != nil {
		t.Errorf("Free() unexpected error: %+v", err)
	}

	// Free invalid offset
	if err := inmem.Free(198787979); err == nil {
		t.Errorf("Free() expeccting error, got nil")
	}

	// Alloc should re-use freed offset
	secondAllocOffset, err := inmem.Alloc([]byte{1, 2, 3})
	if err != nil {
		t.Fatalf("Alloc() unexpected error: %+v", err)
	}

	if firstAllocOffset != secondAllocOffset {
		t.Errorf("Alloc() expected offset %d to be re-used, but got new offset %d",
			firstAllocOffset, secondAllocOffset)
	}
}
