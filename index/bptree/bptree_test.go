package bptree

import (
	"hash/fnv"
	"log"
	"math/rand"
	"testing"
	"time"
)

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func TestBPlusTree_Random(t *testing.T) {
	start := time.Now()
	count := uint32(10000)

	idx, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("failed to init B+ tree: %v", err)
	}
	defer idx.Close()

	for i := uint32(0); i < count; i++ {
		key := randKey(4)
		val := hash(key)

		if err := idx.Put(key, val); err != nil {
			t.Fatalf("Put() unexpected error: %v", err)
		}
	}
	t.Logf("finished %d Put() in %s", count, time.Since(start))

	start = time.Now()
	c := uint32(0)
	_ = idx.Scan(nil, false, func(key []byte, v uint64) bool {
		c++
		expectedV := hash(key)

		if v != expectedV {
			t.Fatalf(
				"Scan() value check failed for key '%s': read value %d != expected %d",
				key, v, expectedV,
			)
		}
		return false
	})

	t.Logf("finished Scan() for %d keys in %s", c, time.Since(start))
}

func TestBPlusTree_Put(t *testing.T) {
	tree, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("failed to init tree: %v", err)
	}
	defer tree.Close()

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
	tree, err := Open(":memory:", nil)
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
}

func BenchmarkBPlusTree_Put_Get(b *testing.B) {
	tree, err := Open(":memory:", nil)
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

func randKey(n int) []byte {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}

func hash(k []byte) uint64 {
	h := fnv.New64()
	h.Write(k)
	return h.Sum64()
}
