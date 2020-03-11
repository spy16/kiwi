package linearhash

import (
	"encoding/binary"
	"os"
	"reflect"
	"testing"
	"unsafe"
)

func Test_headerFrom(t *testing.T) {
	buf := make([]byte, os.Getpagesize())

	bo := findByteOrder()
	bo.PutUint32(buf[0:4], kiwiMagic)
	bo.PutUint32(buf[4:8], kiwiVersion)
	bo.PutUint32(buf[8:12], uint32(os.Getpagesize()))
	bo.PutUint32(buf[12:16], 1)

	got := headerFrom(buf)
	want := header{
		magic:       kiwiMagic,
		version:     kiwiVersion,
		pageSz:      uint32(os.Getpagesize()),
		bucketCount: 1,
		splitBucket: 0,
	}

	if !reflect.DeepEqual(want, *got) {
		t.Errorf("headerFrom() want=%+v, got=%+v", want, *got)
	}
}

func Test_header_validate(t *testing.T) {
	tests := []struct {
		name    string
		h       header
		wantErr bool
	}{
		{
			name:    "EmptyHeader",
			h:       header{},
			wantErr: true,
		},
		{
			name: "InvalidMagic",
			h: header{
				magic: 0xff,
			},
			wantErr: true,
		},
		{
			name: "InvalidVersion",
			h: header{
				magic:   kiwiMagic,
				version: 0x02,
			},
			wantErr: true,
		},
		{
			name: "PageSizeUnset",
			h: header{
				magic:   kiwiMagic,
				version: kiwiVersion,
				pageSz:  0,
			},
			wantErr: true,
		},
		{
			name: "InvalidBucketCount",
			h: header{
				magic:       kiwiMagic,
				version:     kiwiVersion,
				pageSz:      4096,
				bucketCount: 0,
			},
			wantErr: true,
		},
		{
			name: "ValidHeader",
			h: header{
				magic:       kiwiMagic,
				version:     kiwiVersion,
				pageSz:      4096,
				bucketCount: 10,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.h.validate(); (err != nil) != tt.wantErr {
				t.Errorf("header.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func findByteOrder() binary.ByteOrder {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)
	switch buf {
	case [2]byte{0xCD, 0xAB}:
		return binary.LittleEndian

	case [2]byte{0xAB, 0xCD}:
		return binary.BigEndian

	default:
		panic("unexpected byte order")
	}
}
