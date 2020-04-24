//+build ondisk

// Package index_test contains on-disk tests for different indexing
// schemes. For unit tests, see the appropriate respective indexing
// packages.
package index_test

import (
	"encoding/binary"
	"log"
	"time"

	"github.com/spy16/kiwi/index"
)

func writeALot(index index.Index, count uint32) (time.Duration, error) {
	start := time.Now()
	for i := uint32(0); i < count; i++ {
		key, val := genKV(i)
		_ = index.Put(key, val)
	}
	return time.Since(start), nil
}

func readALot(index index.Index, count uint32) (time.Duration, error) {
	start := time.Now()
	for i := uint32(0); i < count; i++ {
		key, val := genKV(i)

		v, err := index.Get(key)
		if err != nil {
			log.Fatalf("Get('%x') -> %v [i=%d]", key, err, i)
		}

		if v != val {
			log.Fatalf(
				"bad read for key='%x' : actual %d != expected %d",
				key, v, val,
			)
		}
	}
	return time.Since(start), nil
}

func scanALot(scanner index.Scanner, count uint32) (time.Duration, error) {
	start := time.Now()

	c := 0
	err := scanner.Scan(nil, false, func(key []byte, actual uint64) bool {
		_, v := genKV(uint32(c))
		c++

		if v != actual {
			log.Fatalf("value of key '%x' expected to be %d but was %d",
				key, v, actual)
		}
		return false
	})

	if c != int(count) {
		log.Fatalf("expected scan to process %d keys, but did only %d", count, c)
	}

	return time.Since(start), err
}

func genKV(i uint32) ([]byte, uint64) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], i)
	return b[:], uint64(i)
}
