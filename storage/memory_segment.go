package storage

import (
	"fmt"
	"os"
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

func NewMemorySegment() Segment {
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

// TODO: 并发安全
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

	// 落盘
	if ms.Frozen() {
		// segment-mints-maxts
		prefix := fmt.Sprintf("segment-%d-%d-", ms.MinTs(), ms.MaxTs())
		if err := ms.flushToDisk(prefix+"meta", prefix+"data"); err != nil {
			panic(err)
		}

		// 构建 disksegment 索引
	}
}

func (ms *memorySegment) QueryRange(metric string, labels LabelSet, start, end int64) {
	labels = labels.AddMetricName(metric)

	for _, sid := range ms.metricIdx.MatchSids(labels) {
		b, _ := ms.segment.Load(sid)
		series := b.(*Series)
		fmt.Printf("%+v\n", series.labels)
		fmt.Printf("%+v\n", series.store.Get(start, end))
		fmt.Println()
	}
}

func (ms *memorySegment) flushToDisk(metaFile, dataFile string) error {
	metaBytes, dataBytes, err := ms.marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal segment: %s", err.Error())
	}

	if isFileExist(metaFile) {
		return fmt.Errorf("%s metafile is exist", metaFile)
	}
	metaFd, err := os.OpenFile(metaFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}

	metaFd.Write(metaBytes)
	defer metaFd.Close()

	if isFileExist(dataFile) {
		return fmt.Errorf("%s datafile is exist", dataFile)
	}
	dataFd, err := os.OpenFile(dataFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}

	dataFd.Write(dataBytes)
	defer dataFd.Close()

	mm := Metadata{}
	ms.metaSerializer.Unmarshal(metaBytes, &mm)
	fmt.Printf("%+v\n", mm)

	return nil
}

func (ms *memorySegment) marshal() ([]byte, []byte, error) {
	sids := make(map[string]uint32)
	dataBytes := make([]byte, 0)

	startOffset := 0
	size := 0

	meta := Metadata{}
	ms.segment.Range(func(key, value interface{}) bool {
		sid := key.(string)
		sids[sid] = uint32(size)
		size++

		bs := value.(*Series).store.Bytes()
		dataBytes = append(dataBytes, bs...)

		endOffset := startOffset + len(bs)
		meta.Series = append(meta.Series, metaSeries{
			Sid:         key.(string),
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
		meta.Labels = labelIdx
	})

	metaBytes, err := ms.metaSerializer.Marshal(meta)
	if err != nil {
		return nil, nil, err
	}
	return metaBytes, dataBytes, nil
}
