package io

import (
	"os"
	"reflect"
	"testing"
)

func TestPager(t *testing.T) {
	t.Parallel()

	p, err := Open(InMemoryFileName, os.Getpagesize(), false, os.ModePerm)
	if err != nil {
		t.Fatalf("Open() unexpected error: %v", err)
	}
	p.pageSize = 5 // to simplify data assertions

	if got := p.Count(); got != 0 {
		t.Errorf("Count() expected 0, got %d", got)
	}

	if err := p.Write(0, []byte("helo")); err == nil {
		t.Errorf("Write() expected error on Write() with no pages")
	}

	id, err := p.Alloc(1)
	if err != nil {
		t.Errorf("Alloc() unexpected error: %#v", err)
	}

	if id != 0 {
		t.Errorf("Alloc() expected first allocation to return id=0, got id=%d", id)
	}

	writeData := []byte("hello") // one page worth of data

	if err := p.Write(0, []byte("aaaaaaaaaaaaaaaaaaaaaa")); err == nil {
		t.Errorf("Write() expected error when writing data larger than a page")
	}

	if err := p.Write(0, writeData); err != nil {
		t.Errorf("Write() unexpected error: %#v", err)
	}

	readData, err := p.Read(0)
	if err != nil {
		t.Errorf("Read(0) unexpected error: %#v", err)
	}

	if !reflect.DeepEqual(writeData, readData) {
		t.Errorf(
			"Read(0) returned unexpected data. want=%#v got=%#v",
			writeData, readData,
		)
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close() unexpected error: %#v", err)
	}
}
