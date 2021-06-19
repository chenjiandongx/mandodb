package storage

import (
	"fmt"
	"os"
	"sync"
)

// TODO: list
// * 处理 Outdated 数据 -> skiplist
// * 归档数据使用 snappy 压缩
// * 磁盘文件合并 参考 leveldb
// * WAL 做灾备

type DataPoint struct {
	Ts    int64
	Value float64
}

func joinSeparator(a, b interface{}) string {
	const separator = "/-/"
	return fmt.Sprintf("%v%s%v", a, separator, b)
}

type Row struct {
	Metric    string
	Labels    LabelSet
	DataPoint DataPoint
}

func (r Row) ID() string {
	return joinSeparator(r.Metric, r.Labels.Hash())
}

type TSDB struct {
	segs *SegmentList
	mut  sync.Mutex
}

func (tsdb *TSDB) InsertRow(row *Row) error {
	tsdb.mut.Lock()
	if tsdb.segs.Head().Frozen() {
		prefix := fmt.Sprintf("segment-%d-%d.", tsdb.segs.Head().MinTs(), tsdb.segs.Head().MaxTs())
		meta, err := tsdb.flushToDisk(tsdb.segs.Head())
		if err != nil {
			return fmt.Errorf("failed to flush data to disk, %v", err)
		}

		mf, err := openMmapFile(prefix + "data")
		if err != nil {
			return fmt.Errorf("failed to make a mmap file, %v", err)
		}

		tsdb.segs.Add(newDiskSegment(mf, meta, tsdb.segs.Head().MinTs(), tsdb.segs.Head().MaxTs()))

		newseg := newMemorySegment()
		tsdb.segs.Add(newseg)
		tsdb.segs.head = newseg
	}
	tsdb.mut.Unlock()

	tsdb.segs.head.InsertRow(row)
	return nil
}

func (tsdb *TSDB) QueryRange(metric string, labels LabelSet, start, end int64) {
	labels = append(labels, Label{Name: metricName, Value: metric})

	ret := tsdb.segs.Get(start, end)
	for _, r := range ret {
		r.QueryRange(labels, start, end)
	}
}

func (tsdb *TSDB) flushToDisk(segment Segment) (*Metadata, error) {
	metaBytes, dataBytes, err := segment.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal segment: %s", err.Error())
	}

	prefix := fmt.Sprintf("segment-%d-%d.", segment.MinTs(), segment.MaxTs())
	metaFile, dataFile := prefix+"meta", prefix+"data"

	if isFileExist(metaFile) {
		return nil, fmt.Errorf("%s metafile is exist", metaFile)
	}
	metaFd, err := os.OpenFile(metaFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	metaFd.Write(metaBytes)
	defer metaFd.Close()

	if isFileExist(dataFile) {
		return nil, fmt.Errorf("%s datafile is exist", dataFile)
	}
	dataFd, err := os.OpenFile(dataFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	dataFd.Write(dataBytes)
	defer dataFd.Close()

	md := Metadata{}
	if err = segment.Unmarshal(metaBytes, &md); err != nil {
		return nil, err
	}

	return &md, nil
}

func OpenTSDB() *TSDB {
	return &TSDB{segs: newSegmentList()}
}
