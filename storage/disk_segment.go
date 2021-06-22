package storage

import (
	"bytes"

	"github.com/dgryski/go-tsz"

	"github.com/chenjiandongx/mandodb/toolkit/mmap"
)

type diskSegment struct {
	mf       *mmap.MmapFile
	indexMap *diskIndexMap
	series   []metaSeries

	minTs int64
	maxTs int64
}

func newDiskSegment(mf *mmap.MmapFile, meta *Metadata, minTs, maxTs int64) Segment {
	return &diskSegment{
		mf:       mf,
		series:   meta.Series,
		indexMap: newDiskIndexMap(meta.Labels),
		minTs:    minTs,
		maxTs:    maxTs,
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

func (ds *diskSegment) Type() SegmentType {
	return DiskSegmentType
}

func (ds *diskSegment) Close() error {
	return ds.mf.Close()
}

func (ds *diskSegment) Marshal() ([]byte, []byte, error) {
	return nil, nil, nil
}

func (ds *diskSegment) InsertRows(_ []*Row) {
	panic("BUG: disk segments are not mutable")
}

func (ds *diskSegment) QueryRange(labels LabelSet, start, end int64) ([]MetricRet, error) {
	sids := ds.indexMap.MatchSids(labels)

	ret := make([]MetricRet, 0)
	for _, sid := range sids {
		startOffset := ds.series[sid].StartOffset
		endOffset := ds.series[sid].EndOffset

		reader := bytes.NewReader(ds.mf.Bytes())
		dataBytes := make([]byte, endOffset-startOffset)
		_, err := reader.ReadAt(dataBytes, int64(startOffset))
		if err != nil {
			return nil, err
		}

		iter, err := tsz.NewIterator(dataBytes)
		if err != nil {
			return nil, err
		}

		dps := make([]DataPoint, 0)
		for iter.Next() {
			ts, val := iter.Values()
			if ts > uint32(end) {
				break
			}

			if ts >= uint32(start) && ts <= uint32(end) {
				dps = append(dps, DataPoint{Ts: int64(ts), Value: val})
			}
		}

		ret = append(ret, MetricRet{
			DataPoints: dps,
			Labels:     ds.indexMap.MatchLabels(ds.series[sid].Labels...),
		})
	}

	return ret, nil
}
