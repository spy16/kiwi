package bptree

import (
	"log"
	"testing"
	"time"

	"github.com/spy16/kiwi/io"
)

func TestBPlusTree_Put_Get(t *testing.T) {
	tree, err := Open(":memory:", false, 0)
	if err != nil {
		t.Fatalf("failed to create in-memory tree: %#v", err)
	}
	defer tree.Close()

	if tree.size != 0 {
		t.Errorf("expected tree size to be 0, not %d", tree.size)
	}

	insertKeys := 10000

	writeLot(t, tree, insertKeys)
	if tree.Size() != int64(insertKeys) {
		t.Errorf("expected tree size to be %d, not %d", insertKeys, tree.Size())
	}

	readCheck(t, tree, insertKeys)
	scanLot(t, tree, insertKeys)
}

func BenchmarkBPlusTree_Put_Get(b *testing.B) {
	tree, err := Open(io.InMemoryFileName, false, 0)
	if err != nil {
		log.Fatalf("failed to init tree: %#v", err)
	}
	defer tree.Close()

	b.Run("Put", func(b *testing.B) {
		var d [4]byte
		for i := 0; i < b.N; i++ {
			d[0] = byte(i >> 24)
			d[1] = byte(i >> 16)
			d[2] = byte(i >> 8)
			d[3] = byte(i)
			_ = tree.Put(d[:], uint64(i))
		}
	})

	b.Run("Get", func(b *testing.B) {
		var d [4]byte
		for i := 0; i < b.N; i++ {
			d[0] = byte(i >> 24)
			d[1] = byte(i >> 16)
			d[2] = byte(i >> 8)
			d[3] = byte(i)
			_, _ = tree.Get(d[:])
		}
	})
}

func readCheck(t *testing.T, tree *BPlusTree, count int) {
	start := time.Now()

	b := make([]byte, 4)
	for i := uint32(0); i < uint32(count); i++ {
		b[0] = byte(i >> 24)
		b[1] = byte(i >> 16)
		b[2] = byte(i >> 8)
		b[3] = byte(i)

		v, err := tree.Get(b)
		if err != nil {
			t.Fatalf("Get('%x') unexpected error: %#v", b, err)
		}

		if v != uint64(i) {
			t.Fatalf("Get('%x'): %d != %d", b, v, uint64(i))
		}
	}
	t.Logf("read %d keys in %s", tree.Size(), time.Since(start))
}

func scanLot(t *testing.T, tree *BPlusTree, count int) {
	start := time.Now()
	scanned := uint64(0)
	_ = tree.Scan(nil, func(key []byte, v uint64) error {
		if v != scanned {
			t.Fatalf("bad scan for '%x': %d != %d", key, v, scanned)
		}
		scanned++
		return nil
	})

	if int(scanned) != count {
		t.Errorf("expected %d keys to be scanned, but scanned only %d", scanned, count)
	}
	t.Logf("scanned %d keys in %s", count, time.Since(start))
}

func writeLot(t *testing.T, tree *BPlusTree, count int) {
	start := time.Now()
	b := make([]byte, 4)
	for i := uint32(0); i < uint32(count); i++ {
		b[0] = byte(i >> 24)
		b[1] = byte(i >> 16)
		b[2] = byte(i >> 8)
		b[3] = byte(i)

		if err := tree.Put(b, uint64(i)); err != nil {
			panic(err)
		}
	}
	t.Logf("inserted %d (expected=%d) keys in %s", tree.Size(), count, time.Since(start))
}
