package linearhash

type bucket struct {
}

type indexEntry struct {
	Hash     uint64
	Checksum uint64
	KeySz    uint64
	ValSz    uint64
	BlobID   uint64
	Key      []byte
}
