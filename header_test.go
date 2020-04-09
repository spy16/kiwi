package kiwi

import (
	"bytes"
	"testing"
)

var sampleHeaderBytes = []byte{
	0x6b, 0x69, 0x77, 0x69, // magic
	0x0, 0x0, 0x0, 0x1, // version
	0x0, 0x1, // flags
	0x10, 0x0, // page-size
}

var sampleHeader = header{
	magic:   kiwiMagic,
	version: dbVersion,
	backend: 0x01,
	pageSz:  4096,
}

func Test_header_MarshalBinary(t *testing.T) {
	got, err := sampleHeader.MarshalBinary()
	if err != nil {
		t.Errorf("MarshalBinary() unexpected error: %v", err)
	}

	if !bytes.Equal(got, sampleHeaderBytes) {
		t.Errorf("MarshalBinary() want=%#v, got=%#v", sampleHeaderBytes, got)
	}
}

func Test_header_UnmarshalBinary(t *testing.T) {
	h := header{}
	if err := h.UnmarshalBinary(sampleHeaderBytes); err != nil {
		t.Errorf("UnmarshalBinary() unexpected error: %v", err)
	}
	if h != sampleHeader {
		t.Errorf("UnmarshalBinary() want=%#v, got=%#v", sampleHeaderBytes, h)
	}
}
