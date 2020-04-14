package bptree

import (
	"encoding"
	"testing"
)

var (
	_ encoding.BinaryMarshaler   = (*header)(nil)
	_ encoding.BinaryUnmarshaler = (*header)(nil)
)

func Test_header(t *testing.T) {
	t.Parallel()

	t.Run("NilHeader", func(t *testing.T) {
		var h *header
		_, err := h.MarshalBinary()
		if err == nil {
			t.Errorf("expecting error, got nil")
		}

		err = h.UnmarshalBinary([]byte{})
		if err == nil {
			t.Errorf("expecting error, got nil")
		}
	})

	t.Run("Valid", func(t *testing.T) {
		original := header{
			magic:   0xABCDEF,
			version: 0x1,
			pageSz:  0x1000,
			flags:   0xFF,
			order:   0x01,
		}

		data, err := original.MarshalBinary()
		if err != nil {
			t.Errorf("MarshalBinary() unexpected error: %#v", err)
		}

		if len(data) == 0 {
			t.Errorf("byte conversion failed")
		}

		got := header{}

		if err := got.UnmarshalBinary([]byte{}); err == nil {
			t.Errorf("UnmarshalBinary() expecting error for empty data, got nil")
		}

		if err := got.UnmarshalBinary(data); err != nil {
			t.Errorf("UnmarshalBinary() unexpected error: %#v", err)
		}

		if err := got.Validate(); err != nil {
			t.Errorf("Validate() unexpected error: %#v", err)
		}

		if original != got {
			t.Errorf("byte-header conversion failed. expected=%#v, got=%#v", original, got)
		}
	})
}

func Benchmark_header_MarshalBinary(b *testing.B) {
	h := header{}
	for i := 0; i < b.N; i++ {
		_, _ = h.MarshalBinary()
	}
}

func Benchmark_header_UnmarshalBinary(b *testing.B) {
	h := header{}
	data, err := h.MarshalBinary()
	if err != nil {
		b.Fatalf("failed to marshal: %#v", err)
	}

	for i := 0; i < b.N; i++ {
		_ = h.UnmarshalBinary(data)
	}
}
