package storage

import (
	"encoding/json"
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

	labelVs *labelValueSet

	minTs int64
	maxTs int64

	seriesCount     int64
	dataPointsCount int64
}

func newMemorySegment() Segment {
	return &memorySegment{
		indexMap: newMemoryIndexMap(),
		labelVs:  newLabelValueSet(),
	}
}

func (ms *memorySegment) getOrCreateSeries(row *Row) *Series {
	v, ok := ms.segment.Load(row.ID())
	if ok {
		return v.(*Series)
	}

	atomic.AddInt64(&ms.seriesCount, 1)
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

func (ms *memorySegment) QueryLabelValues(label string) []string {
	return ms.labelVs.Get(label)
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

func (ms *memorySegment) Load() Segment {
	return ms
}

func (ms *memorySegment) InsertRows(rows []*Row) {
	for _, row := range rows {
		ms.labelVs.Set(metricName, row.Metric)
		for _, label := range row.Labels {
			ms.labelVs.Set(label.Name, label.Value)
		}

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
		atomic.AddInt64(&ms.dataPointsCount, 1)
		ms.indexMap.UpdateIndex(row.ID(), row.Labels)
	}
}

func (ms *memorySegment) QuerySeries(labels LabelSet) ([]LabelSet, error) {
	matchSids := ms.indexMap.MatchSids(ms.labelVs, labels)
	ret := make([]LabelSet, 0)
	for _, sid := range matchSids {
		b, _ := ms.segment.Load(sid)
		series := b.(*Series)

		ret = append(ret, series.labels)
	}

	return ret, nil
}

func (ms *memorySegment) QueryRange(labels LabelSet, start, end int64) ([]MetricRet, error) {
	matchSids := ms.indexMap.MatchSids(ms.labelVs, labels)
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

func (ms *memorySegment) Marshal() ([]byte, []byte, []byte, error) {
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

	// key: Label.MarshalName()
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
		return nil, nil, nil, err
	}

	desc := &Desc{
		SeriesCount:     ms.seriesCount,
		DataPointsCount: ms.dataPointsCount,
		MaxTs:           ms.maxTs,
		MinTs:           ms.minTs,
	}

	descBytes, _ := json.MarshalIndent(desc, "", "    ")

	return metaBytes, dataBuf, descBytes, nil
}

func writeToDisk(segment Segment) (*Metadata, error) {
	metaBytes, dataBytes, descBytes, err := segment.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal segment: %s", err.Error())
	}

	writeFile := func(f string, data []byte) error {
		if isFileExist(f) {
			return fmt.Errorf("%s file is exist", f)
		}

		metaFd, err := os.OpenFile(f, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return err
		}
		defer metaFd.Close()

		_, err = metaFd.Write(data)
		return err
	}

	prefix := filePrefix(segment.MinTs(), segment.MaxTs())
	if err := writeFile(prefix+"meta", metaBytes); err != nil {
		return nil, err
	}

	if err := writeFile(prefix+"data", dataBytes); err != nil {
		return nil, err
	}

	if err := writeFile(prefix+"json", descBytes); err != nil {
		return nil, err
	}

	md := Metadata{}
	if err = UnmarshalMeta(metaBytes, &md); err != nil {
		return nil, err
	}

	return &md, nil
}
