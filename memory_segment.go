package mandodb

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/chenjiandongx/mandodb/pkg/sortedlist"
)

type memorySegment struct {
	once     sync.Once
	segment  sync.Map
	indexMap *memoryIndexMap
	labelVs  *labelValueSet

	outdated    map[string]sortedlist.List
	outdatedMut sync.Mutex

	minTs int64
	maxTs int64

	seriesCount     int64
	dataPointsCount int64
}

func newMemorySegment() Segment {
	return &memorySegment{
		indexMap: newMemoryIndexMap(),
		labelVs:  newLabelValueSet(),
		outdated: make(map[string]sortedlist.List),
		minTs:    math.MaxInt64,
		maxTs:    math.MinInt64,
	}
}

func (ms *memorySegment) getOrCreateSeries(row *Row) *memorySeries {
	v, ok := ms.segment.Load(row.ID())
	if ok {
		return v.(*memorySeries)
	}

	atomic.AddInt64(&ms.seriesCount, 1)
	newSeries := newSeries(row)
	ms.segment.Store(row.ID(), newSeries)

	return newSeries
}

func (ms *memorySegment) MinTs() int64 {
	return atomic.LoadInt64(&ms.minTs)
}

func (ms *memorySegment) MaxTs() int64 {
	return atomic.LoadInt64(&ms.maxTs)
}

func (ms *memorySegment) Frozen() bool {
	if globalOpts.onlyMemoryMode {
		return false
	}

	return ms.MaxTs()-ms.MinTs() > int64(globalOpts.segmentDuration.Seconds())
}

func (ms *memorySegment) Type() SegmentType {
	return MemorySegmentType
}

func (ms *memorySegment) Close() error {
	if ms.dataPointsCount == 0 || globalOpts.onlyMemoryMode {
		return nil
	}

	return writeToDisk(ms)
}

func (ms *memorySegment) Cleanup() error {
	return nil
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
		row.Labels.Sorted()
		series := ms.getOrCreateSeries(row)

		dp := series.Append(&row.Point)

		if dp != nil {
			ms.outdatedMut.Lock()
			if _, ok := ms.outdated[row.ID()]; !ok {
				ms.outdated[row.ID()] = sortedlist.NewTree()
			}
			ms.outdated[row.ID()].Add(row.Point.Ts, row.Point)
			ms.outdatedMut.Unlock()
		}

		if atomic.LoadInt64(&ms.minTs) >= row.Point.Ts {
			atomic.StoreInt64(&ms.minTs, row.Point.Ts)
		}
		if atomic.LoadInt64(&ms.maxTs) <= row.Point.Ts {
			atomic.StoreInt64(&ms.maxTs, row.Point.Ts)
		}
		atomic.AddInt64(&ms.dataPointsCount, 1)
		ms.indexMap.UpdateIndex(row.ID(), row.Labels)
	}
}

func (ms *memorySegment) QueryLabelValues(label string) []string {
	return ms.labelVs.Get(label)
}

func (ms *memorySegment) QuerySeries(lms LabelMatcherSet) ([]LabelSet, error) {
	matchSids := ms.indexMap.MatchSids(ms.labelVs, lms)
	ret := make([]LabelSet, 0)
	for _, sid := range matchSids {
		b, _ := ms.segment.Load(sid)
		series := b.(*memorySeries)

		ret = append(ret, series.labels)
	}

	return ret, nil
}

func (ms *memorySegment) QueryRange(lms LabelMatcherSet, start, end int64) ([]MetricRet, error) {
	matchSids := ms.indexMap.MatchSids(ms.labelVs, lms)
	ret := make([]MetricRet, 0, len(matchSids))
	for _, sid := range matchSids {
		b, _ := ms.segment.Load(sid)
		series := b.(*memorySeries)

		points := series.Get(start, end)

		ms.outdatedMut.Lock()
		v, ok := ms.outdated[sid]
		if ok {
			iter := v.Range(start, end)
			for iter.Next() {
				points = append(points, iter.Value().(Point))
			}
		}
		ms.outdatedMut.Unlock()

		ret = append(ret, MetricRet{
			Labels: series.labels,
			Points: points,
		})
	}

	return ret, nil
}

func (ms *memorySegment) Marshal() ([]byte, []byte, error) {
	sids := make(map[string]uint32)

	startOffset := 0
	size := 0

	dataBuf := make([]byte, 0)

	// TOC 占位符 用于后面标记 dataBytes / metaBytes 长度
	dataBuf = append(dataBuf, make([]byte, uint64Size*2)...)
	meta := Metadata{MinTs: ms.minTs, MaxTs: ms.maxTs}

	// key: sid
	// value: series entity
	ms.segment.Range(func(key, value interface{}) bool {
		sid := key.(string)
		sids[sid] = uint32(size)
		size++

		series := value.(*memorySeries)
		meta.sidRelatedLabels = append(meta.sidRelatedLabels, series.labels)

		ms.outdatedMut.Lock()
		v, ok := ms.outdated[sid]
		ms.outdatedMut.Unlock()

		var dataBytes []byte
		if ok {
			dataBytes = ByteCompress(series.MergeOutdatedList(v).Bytes())
		} else {
			dataBytes = ByteCompress(series.Bytes())
		}

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

		sort.Slice(l, func(i, j int) bool {
			return l[i] < l[j]
		})
		labelIdx = append(labelIdx, seriesWithLabel{Name: key, Sids: l})
	})
	meta.Labels = labelIdx

	metaBytes, err := MarshalMeta(meta)
	if err != nil {
		return nil, nil, err
	}
	metalen := len(metaBytes)

	desc := &Desc{
		SeriesCount:     ms.seriesCount,
		DataPointsCount: ms.dataPointsCount,
		MaxTs:           ms.maxTs,
		MinTs:           ms.minTs,
	}

	descBytes, _ := json.MarshalIndent(desc, "", "    ")

	dataLen := len(dataBuf) - (uint64Size * 2)
	dataBuf = append(dataBuf, metaBytes...)

	// TOC 写入
	encf := newEncbuf()
	encf.MarshalUint64(uint64(dataLen))
	dataLenBs := encf.Bytes()
	copy(dataBuf[:uint64Size], dataLenBs[:uint64Size])

	encf.Reset()

	encf.MarshalUint64(uint64(metalen))
	metaLenBs := encf.Bytes()
	copy(dataBuf[uint64Size:uint64Size*2], metaLenBs[:uint64Size])

	return dataBuf, descBytes, nil
}

func mkdir(d string) {
	d = path.Join(globalOpts.dataPath, d)
	if _, err := os.Stat(d); !os.IsNotExist(err) {
		return
	}

	if err := os.MkdirAll(d, os.ModePerm); err != nil {
		panic(fmt.Sprintf("BUG: failed to create dir: %s", d))
	}
}

func writeToDisk(segment *memorySegment) error {
	dataBytes, descBytes, err := segment.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal segment: %s", err.Error())
	}

	writeFile := func(f string, data []byte) error {
		if isFileExist(f) {
			return fmt.Errorf("%s file is already exists", f)
		}

		fd, err := os.OpenFile(f, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return err
		}
		defer fd.Close()

		_, err = fd.Write(data)
		return err
	}

	dn := dirname(segment.MinTs(), segment.MaxTs())
	mkdir(dn)

	if err := writeFile(path.Join(dn, "data"), dataBytes); err != nil {
		return err
	}

	// 这里的 meta.json 只是描述了一些简单的信息 并非全局定义的 MetaData
	if err := writeFile(path.Join(dn, "meta.json"), descBytes); err != nil {
		return err
	}

	return nil
}
