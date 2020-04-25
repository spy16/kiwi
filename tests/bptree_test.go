//+build ondisk

package index_test

import (
	"os"
	"testing"

	"github.com/spy16/kiwi/index/bptree"
)

func TestBPlusTree(t *testing.T) {
	fileName := "kiwi_bptree.idx"
	_ = os.Remove(fileName)

	t.Logf("using file '%s'...", fileName)

	tree, err := bptree.Open(fileName, &bptree.Options{
		ReadOnly:   false,
		FileMode:   0664,
		MaxKeySize: 4,
		PageSize:   os.Getpagesize(),
		PreAlloc:   100,
	})
	if err != nil {
		t.Fatalf("failed to init B+ tree: %v", err)
	}
	defer func() {
		_ = tree.Close()
		_ = os.Remove(fileName)
	}()

	count := uint32(10000)
	writeTime, err := writeALot(tree, count)
	if err != nil {
		t.Errorf("error while Put(): %v", err)
	}
	t.Logf("took %s to Put %d entris", writeTime, count)

	scanTime, err := scanALot(tree, count)
	if err != nil {
		t.Errorf("error while Scan(): %v", err)
	}
	t.Logf("took %s to Scan %d entris", scanTime, count)

	readTime, err := readALot(tree, count)
	if err != nil {
		t.Errorf("error while Get(): %v", err)
	}
	t.Logf("took %s to Get %d entris", readTime, count)

}
