package storage

import (
	"sync"

	"github.com/dgryski/go-tsz"
)

type diskSegment struct {
	segment   sync.Map
	metricIdx *indexMap

	minTs int64
	maxTs int64
}

func newDiskSegment() Segment {
	return &diskSegment{}
}

func (ds *diskSegment) MinTs() int64 {
	return ds.minTs
}

func (ds *diskSegment) MaxTs() int64 {
	return ds.maxTs
}

func (ds *diskSegment) Frozen() bool {
	return false
}

func (ds *diskSegment) InsertRow(_ *Row) {
	// TODO
	panic("")
}

func (ds *diskSegment) QueryRange(metric string, labels LabelSet, start, end int64) {
	iter, err := tsz.NewIterator(nil)
	if err != nil {

	}

	_ = iter
}
