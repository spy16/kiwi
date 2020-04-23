//+build ondisk

package index_test

import (
	"testing"

	"github.com/spy16/kiwi/index/bptree"
)

func TestBPlusTree(t *testing.T) {
	fileName := "kiwi_bptree.idx"
	p, cleanup, err := createPager(fileName)
	if err != nil {
		t.Fatalf("failed to create page file '%s': %v", fileName, err)
	}
	defer cleanup()

	t.Logf("using file '%s'...", fileName)

	tree, err := bptree.New(p, nil)
	if err != nil {
		t.Fatalf("failed to init B+ tree: %v", err)
	}
	defer tree.Close()

	count := uint32(10000)
	writeTime, err := writeALot(tree, count)
	if err != nil {
		t.Errorf("error while Put(): %v", err)
	}
	t.Logf("took %s to Put %d entris", writeTime, count)

	readTime, err := readALot(tree, count)
	if err != nil {
		t.Errorf("error while Put(): %v", err)
	}
	t.Logf("took %s to Get %d entris", readTime, count)

	scanTime, err := scanALot(tree, count)
	if err != nil {
		t.Errorf("error while Put(): %v", err)
	}
	t.Logf("took %s to Scan %d entris", scanTime, count)

	t.Logf("I/O stats: %s", p.Stats())
}
