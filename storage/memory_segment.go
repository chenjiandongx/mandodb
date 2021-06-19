package storage

import (
	"sort"
	"sync"
	"sync/atomic"
)

type memorySegment struct {
	once      sync.Once
	segment   sync.Map
	metricIdx *indexMap

	minTs int64
	maxTs int64

	metaSerializer MetaSerializer
}

func newMemorySegment() Segment {
	segment := &memorySegment{
		metricIdx:      newIndexMap(),
		metaSerializer: &binaryMetaSerializer{},
	}

	return segment
}

func (ms *memorySegment) getOrCreateSeries(row *Row) *Series {
	v, ok := ms.segment.Load(row.ID())
	if ok {
		return v.(*Series)
	}

	newSeries := newSeries(row)
	ms.segment.Store(row.ID(), newSeries)

	return newSeries
}

func (ms *memorySegment) MinTs() int64 {
	return ms.minTs
}

func (ms *memorySegment) MaxTs() int64 {
	return ms.maxTs
}

func (ms *memorySegment) Frozen() bool {
	return ms.MaxTs()-ms.MinTs() >= 600
}

func (ms *memorySegment) Unmarshal(bs []byte, metadata *Metadata) error {
	return ms.metaSerializer.Unmarshal(bs, metadata)
}

func (ms *memorySegment) Type() SegmentType {
	return MemorySegmentType
}

func (ms *memorySegment) InsertRow(row *Row) {
	row.Labels = row.Labels.AddMetricName(row.Metric)
	series := ms.getOrCreateSeries(row)
	series.store.Append(row.DataPoint)

	ms.once.Do(func() {
		ms.minTs = row.DataPoint.Ts
		ms.maxTs = row.DataPoint.Ts
	})

	if atomic.LoadInt64(&ms.maxTs) < row.DataPoint.Ts {
		atomic.SwapInt64(&ms.maxTs, row.DataPoint.Ts)
	}
	ms.metricIdx.UpdateIndex(row.ID(), row.Labels)
}

func (ms *memorySegment) QueryRange(labels LabelSet, start, end int64) []MetricRet {
	ret := make([]MetricRet, 0)
	for _, sid := range ms.metricIdx.MatchSids(labels) {
		b, _ := ms.segment.Load(sid)
		series := b.(*Series)
		ret = append(ret, MetricRet{
			Labels:     series.labels,
			DataPoints: series.store.Get(start, end),
		})
	}

	return ret
}

func (ms *memorySegment) Marshal() ([]byte, []byte, error) {
	sids := make(map[string]uint32)

	startOffset := 0
	size := 0

	dataBuf := make([]byte, 0)
	meta := Metadata{}

	ms.segment.Range(func(key, value interface{}) bool {
		sid := key.(string)
		sids[sid] = uint32(size)
		size++

		series := value.(*Series)

		labelBytes := series.labels.Bytes()
		dataBuf = append(dataBuf, labelBytes...)

		dataBytes := series.store.Bytes()
		dataBuf = append(dataBuf, dataBytes...)

		endOffset := startOffset + len(labelBytes) + len(dataBytes)
		meta.Series = append(meta.Series, metaSeries{
			Sid:         key.(string),
			LabelLen:    uint64(len(labelBytes)),
			StartOffset: uint64(startOffset),
			EndOffset:   uint64(endOffset),
		})
		startOffset = endOffset

		return true
	})

	labelIdx := make(map[string][]uint32)
	ms.metricIdx.Range(func(k string, v *sidSet) {
		l := make([]uint32, 0)
		for _, s := range v.List() {
			l = append(l, sids[s])
		}

		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		labelIdx[k] = l
	})
	meta.Labels = labelIdx

	metaBytes, err := ms.metaSerializer.Marshal(meta)
	if err != nil {
		return nil, nil, err
	}

	return metaBytes, dataBuf, nil
}
