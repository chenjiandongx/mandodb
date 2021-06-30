package storage

import (
	"bytes"
	"io/ioutil"
	"time"

	"github.com/chenjiandongx/logger"
	"github.com/dgryski/go-tsz"

	"github.com/chenjiandongx/mandodb/lib/mmap"
)

type diskSegment struct {
	dataFd *mmap.MmapFile
	metaF  string
	load   bool

	labelVs  *labelValueSet
	indexMap *diskIndexMap
	series   []metaSeries

	minTs int64
	maxTs int64

	seriesCount     int64
	dataPointsCount int64
}

func newDiskSegment(mf *mmap.MmapFile, metaF string, minTs, maxTs int64) Segment {
	return &diskSegment{
		dataFd:  mf,
		metaF:   metaF,
		minTs:   minTs,
		maxTs:   maxTs,
		labelVs: newLabelValueSet(),
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
	return ds.dataFd.Close()
}

func (ds *diskSegment) Load() Segment {
	if ds.load {
		return ds
	}

	t0 := time.Now()
	bs, err := ioutil.ReadFile(ds.metaF)
	if err != nil {
		logger.Errorf("failed to read file %s: %v", ds.metaF, err)
		return ds
	}

	meta := Metadata{}
	if err := UnmarshalMeta(bs, &meta); err != nil {
		logger.Errorf("failed to unmarshal meta: %v", err)
		return ds
	}

	for _, label := range meta.Labels {
		k, v := unmarshalLabelName(label.Name)
		if k != "" && v != "" {
			ds.labelVs.Set(k, v)
		}
	}

	ds.indexMap = newDiskIndexMap(meta.Labels)
	ds.series = meta.Series
	ds.load = true

	logger.Infof("load disk segment %s, take: %v", ds.metaF, time.Since(t0))
	return ds
}

func (ds *diskSegment) Marshal() ([]byte, []byte, []byte, error) {
	return nil, nil, nil, nil
}

func (ds *diskSegment) QueryLabelValues(label string) []string {
	return ds.labelVs.Get(label)
}

func (ds *diskSegment) InsertRows(_ []*Row) {
	panic("BUG: disk segments are not mutable")
}

func (ds *diskSegment) QuerySeries(labels LabelSet) ([]LabelSet, error) {
	sids := ds.indexMap.MatchSids(ds.labelVs, labels)
	ret := make([]LabelSet, 0)

	for _, sid := range sids {
		ret = append(ret, ds.indexMap.MatchLabels(ds.series[sid].Labels...))
	}

	return ret, nil
}

func (ds *diskSegment) QueryRange(labels LabelSet, start, end int64) ([]MetricRet, error) {
	sids := ds.indexMap.MatchSids(ds.labelVs, labels)

	ret := make([]MetricRet, 0)
	for _, sid := range sids {
		startOffset := ds.series[sid].StartOffset
		endOffset := ds.series[sid].EndOffset

		reader := bytes.NewReader(ds.dataFd.Bytes())
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

		lbs := ds.indexMap.MatchLabels(ds.series[sid].Labels...)
		lbs = append(lbs, Label{Name: metricName, Value: labels.Metric()})
		ret = append(ret, MetricRet{
			DataPoints: dps,
			Labels:     lbs,
		})
	}

	return ret, nil
}
