package storage

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

type memorySegment struct {
	once     sync.Once
	segment  sync.Map
	indexMap *memoryIndexMap

	minTs int64
	maxTs int64
}

func newMemorySegment() Segment {
	return &memorySegment{
		indexMap: newMemoryIndexMap(),
	}
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
	// TODO: 动态配置
	return ms.MaxTs()-ms.MinTs() >= 3600
}

func (ms *memorySegment) Type() SegmentType {
	return MemorySegmentType
}

func (ms *memorySegment) Close() error {
	// 内存无数据就不持久化了
	if ms.MinTs() == 0 && ms.MaxTs() == 0 {
		return nil
	}

	_, err := writeToDisk(ms)
	return err
}

func (ms *memorySegment) InsertRows(rows []*Row) {
	for _, row := range rows {
		row.Labels = row.Labels.AddMetricName(row.Metric)
		series := ms.getOrCreateSeries(row)

		outdated := series.store.Append(&row.DataPoint)

		// TODO: 处理乱序数据
		_ = outdated

		ms.once.Do(func() {
			ms.minTs = row.DataPoint.Ts
			ms.maxTs = row.DataPoint.Ts
		})

		if atomic.LoadInt64(&ms.maxTs) < row.DataPoint.Ts {
			atomic.SwapInt64(&ms.maxTs, row.DataPoint.Ts)
		}
		ms.indexMap.UpdateIndex(row.ID(), row.Labels)
	}
}

func (ms *memorySegment) QueryRange(labels LabelSet, start, end int64) ([]MetricRet, error) {
	matchSids := ms.indexMap.MatchSids(labels)
	ret := make([]MetricRet, 0, len(matchSids))
	for _, sid := range matchSids {
		b, _ := ms.segment.Load(sid)
		series := b.(*Series)

		ret = append(ret, MetricRet{
			Labels:     series.labels,
			DataPoints: series.store.Get(start, end),
		})
	}

	return ret, nil
}

func (ms *memorySegment) Marshal() ([]byte, []byte, error) {
	sids := make(map[string]uint32)

	startOffset := 0
	size := 0

	dataBuf := make([]byte, 0)
	meta := Metadata{MinTs: ms.minTs, MaxTs: ms.maxTs}

	// key: sid
	// value: series entity
	ms.segment.Range(func(key, value interface{}) bool {
		sid := key.(string)
		sids[sid] = uint32(size)
		size++

		series := value.(*Series)
		meta.sidRelatedLabels = append(meta.sidRelatedLabels, series.labels)

		dataBytes := series.store.Bytes()
		dataBuf = append(dataBuf, dataBytes...)

		endOffset := startOffset + len(dataBytes)
		meta.Series = append(meta.Series, metaSeries{
			Sid:         key.(string),
			StartOffset: uint64(startOffset),
			EndOffset:   uint64(endOffset),
		})
		startOffset = endOffset

		return true
	})

	labelIdx := make([]seriesWithLabel, 0)

	// key: Label.String()
	// value: sids...
	ms.indexMap.Range(func(key string, value *memorySidSet) {
		l := make([]uint32, 0)
		for _, s := range value.List() {
			l = append(l, sids[s])
		}

		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		labelIdx = append(labelIdx, seriesWithLabel{Name: key, Sids: l})
	})
	meta.Labels = labelIdx

	metaBytes, err := MarshalMeta(meta)
	if err != nil {
		return nil, nil, err
	}

	return metaBytes, dataBuf, nil
}

func writeToDisk(segment Segment) (*Metadata, error) {
	metaBytes, dataBytes, err := segment.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal segment: %s", err.Error())
	}

	prefix := filePrefix(segment.MinTs(), segment.MaxTs())
	metaFile, dataFile := prefix+"meta", prefix+"data"

	if isFileExist(metaFile) {
		return nil, fmt.Errorf("%s meta file is exist", metaFile)
	}
	metaFd, err := os.OpenFile(metaFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	_, err = metaFd.Write(metaBytes)
	if err != nil {
		return nil, err
	}

	defer metaFd.Close()

	if isFileExist(dataFile) {
		return nil, fmt.Errorf("%s data file is exist", dataFile)
	}
	dataFd, err := os.OpenFile(dataFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	_, err = dataFd.Write(dataBytes)
	if err != nil {
		return nil, err
	}

	defer dataFd.Close()

	md := Metadata{}
	if err = UnmarshalMeta(metaBytes, &md); err != nil {
		return nil, err
	}

	return &md, nil
}
