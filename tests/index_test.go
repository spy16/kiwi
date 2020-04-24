//+build ondisk

// Package index_test contains on-disk tests for different indexing
// schemes. For unit tests, see the appropriate respective indexing
// packages.
package index_test

import (
	"log"
	"os"
	"time"
	"unsafe"

	"github.com/spy16/kiwi/index"
	"github.com/spy16/kiwi/io"
)

func createPager(name string) (*io.Pager, func(), error) {
	p, err := io.Open(name, os.Getpagesize(), false, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = p.Close()
		_ = os.Remove(name)
	}

	return p, cleanup, nil
}

func writeALot(index index.Index, count uint32) (time.Duration, error) {
	start := time.Now()
	for i := uint32(0); i < count; i++ {
		_ = index.Put((*(*[4]byte)(unsafe.Pointer(&i)))[:], uint64(i))
	}
	return time.Since(start), nil
}

func readALot(index index.Index, count uint32) (time.Duration, error) {
	start := time.Now()
	for i := uint32(0); i < count; i++ {
		key := (*(*[4]byte)(unsafe.Pointer(&i)))
		v, _ := index.Get(key[:])
		if v != uint64(i) {
			log.Fatalf(
				"bad read for key='%x' : actual %d != expected %d",
				key, v, uint64(i),
			)
		}
	}
	return time.Since(start), nil
}

func scanALot(scanner index.Scanner, count uint32) (time.Duration, error) {
	start := time.Now()
	err := scanner.Scan(nil, false, func(key []byte, v uint64) bool {
		return false
	})
	return time.Since(start), err
}
