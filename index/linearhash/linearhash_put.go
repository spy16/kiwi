package linearhash

import (
	"github.com/spy16/kiwi/index"
)

// Put inserts the indexing entry into the hash table.
func (idx *LinearHash) Put(key []byte, val uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.isImmutable() {
		return index.ErrImmutable
	}

	return idx.putEntry(entry{key: key, val: val})
}

func (idx *LinearHash) putEntry(e entry) error {
	return nil
}

func (idx *LinearHash) locateSlot(key []byte) (res *bucket, slotID int, err error) {
	hash := idx.hash(key)
	bucketID := idx.bucketIndex(hash)

	bucketPage, err := idx.pager.Read(int(bucketID))
	if err != nil {
		return nil, 0, err
	}

	b := &bucket{}
	if err := b.UnmarshalBinary(bucketPage); err != nil {
		return nil, 0, err
	}

	for b != nil {
		for i := 0; i < int(idx.slotCount); i++ {
			sl := b.slot(i)
			if sl.hash == 0 { // an empty slot
				return b, i, nil
			} else if sl.hash == hash {
				return b, i, nil
			}
		}

		b, err = b.next(idx) // follow the bucket overflow pointer
		if err != nil {
			return nil, 0, err
		}
	}

	return nil, 0, nil
}

type entry struct {
	key []byte
	val uint64
}
