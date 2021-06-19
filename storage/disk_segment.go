package storage

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/dgryski/go-tsz"
)

type diskSegment struct {
	mf        *mmapFile
	metricIdx *indexMap
	series    []metaSeries

	minTs int64
	maxTs int64
}

func newDiskSegment(mf *mmapFile, meta *Metadata, minTs, maxTs int64) Segment {
	return &diskSegment{
		mf:        mf,
		series:    meta.Series,
		metricIdx: buildIndexMapForDisk(meta.Labels),
		minTs:     minTs,
		maxTs:     maxTs,
	}
}

func (ds *diskSegment) MinTs() int64 {
	return ds.minTs
}

func (ds *diskSegment) MaxTs() int64 {
	return ds.maxTs
}

func (ds *diskSegment) Frozen() bool {
	return true
}

func (ds *diskSegment) Marshal() ([]byte, []byte, error) {
	return nil, nil, nil
}

func (ds *diskSegment) Unmarshal(_ []byte, _ *Metadata) error {
	return nil
}

func (ds *diskSegment) Type() SegmentType {
	return DiskSegmentType
}

func (ds *diskSegment) InsertRow(_ *Row) {
	panic("mandodb: disk segments are not mutable")
}

func (ds *diskSegment) QueryRange(labels LabelSet, start, end int64) {
	sids := make([]int, 0)

	for _, sid := range ds.metricIdx.MatchSids(labels) {
		i, _ := strconv.Atoi(sid)
		sids = append(sids, i)
	}

	sort.Ints(sids)
	for _, sid := range sids {
		so := ds.series[sid].StartOffset
		eo := ds.series[sid].EndOffset

		_ = sid

		bs := ds.mf.Bytes()

		bs1 := make([]byte, 1024)
		copy(bs1, bs)
		iter, err := tsz.NewIterator(bs1[so:eo])
		if err != nil {
			panic(err)
		}

		_ = iter

		for iter.Next() {
			ts, val := iter.Values()

			if ts >= uint32(start) && ts <= uint32(end) {
				fmt.Println(ts, val)
				//panic(err)
			}
		}
	}
}
