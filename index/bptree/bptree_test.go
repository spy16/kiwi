package bptree

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/spy16/kiwi/io"
)

func TestBPlusTree_Put(t *testing.T) {
	p, err := io.Open(":memory:", 400, false, 0)
	if err != nil {
		t.Fatalf("failed to init pager: %v", err)
	}
	defer p.Close()

	tree, err := New(p, nil)
	if err != nil {
		t.Fatalf("failed to init tree: %v", err)
	}

	for i := 0; i < 256; i++ {
		if err := tree.Put([]byte{byte(i)}, uint64(i)); err != nil {
			t.Fatalf("failed: %v", err)
		}
	}

	if tree.Size() != 256 {
		t.Errorf("expected tree size to be 256, got %d", tree.Size())
	}

	for i := 0; i < 5; i++ {
		if err := tree.Put([]byte{byte(65 + i)}, uint64(i)); err != nil {
			t.Fatalf("failed: %v", err)
		}
	}

	if tree.Size() != 256 {
		t.Errorf("expected tree size to be 256, got %d", tree.Size())
	}
}

func TestBPlusTree_Put_Get(t *testing.T) {
	p, err := io.Open(":memory:", os.Getpagesize(), false, 0)
	if err != nil {
		t.Fatalf("failed to initialize pager: %#v", err)
	}

	tree, err := New(p, nil)
	if err != nil {
		t.Fatalf("failed to create in-memory tree: %#v", err)
	}
	defer tree.Close()

	if tree.Size() != 0 {
		t.Errorf("expected tree size to be 0, not %d", tree.Size())
	}

	t.Run("Batch", func(t *testing.T) {
		insertKeys := 10000

		writeLot(t, tree, insertKeys)
		if tree.Size() != int64(insertKeys) {
			t.Errorf("expected tree size to be %d, not %d", insertKeys, tree.Size())
		}
		readCheck(t, tree, insertKeys)
		scanLot(t, tree, insertKeys)
	})

	t.Run("Update", func(t *testing.T) {
		if err := tree.Put([]byte("hello"), 12345); err != nil {
			t.Errorf("Put() unexpected error: %#v", err)
		}

		if err := tree.Put([]byte("hello"), 120012); err != nil {
			t.Errorf("Put() unexpected error: %#v", err)
		}

		v, err := tree.Get([]byte("hello"))
		if err != nil {
			t.Errorf("Get('hello') unexpected error: %#v", err)
		}

		if v != 120012 {
			t.Errorf("expected value of key 'hello' to be 120012, not %d", v)
		}
	})

	t.Logf("I/O Stats: %s", p.Stats())
}

func BenchmarkBPlusTree_Put_Get(b *testing.B) {
	p, err := io.Open(":memory:", os.Getpagesize(), false, 0)
	if err != nil {
		b.Fatalf("failed to initialize pager: %#v", err)
	}

	tree, err := New(p, nil)
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
	_ = tree.Scan(nil, false, func(key []byte, v uint64) bool {
		if v != scanned {
			t.Fatalf("bad scan for '%x': %d != %d", key, v, scanned)
		}
		scanned++
		return false
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
