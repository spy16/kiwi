package linearhash

// Stats returns the status information about this LHStore instance..
func (lhs *Store) Stats() Stats {
	return Stats{
		Name:           lhs.index.Name(),
		FileSize:       lhs.size,
		Version:        int(lhs.version),
		PageSize:       int(lhs.pageSz),
		Buckets:        int(lhs.bucketCount),
		ReadOnly:       lhs.readOnly,
		Closed:         lhs.closed,
		SlotsPerBucket: int(lhs.slotCount()),
	}
}

// Stats represents stats about the LHStore instance.
type Stats struct {
	Name           string `json:"name,omitempty"`
	FileSize       int64  `json:"file_size,omitempty"`
	Version        int    `json:"version,omitempty"`
	PageSize       int    `json:"page_size,omitempty"`
	Buckets        int    `json:"buckets,omitempty"`
	SlotsPerBucket int    `json:"slots_per_bucket,omitempty"`
	ReadOnly       bool   `json:"read_only,omitempty"`
	Closed         bool   `json:"closed,omitempty"`
}
