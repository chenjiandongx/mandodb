package storage

import (
	"fmt"
)

// TODO: list
// * 处理 Outdated 数据 -> skiplist
// * 归档数据使用 snappy 压缩
// * 磁盘文件合并 参考 leveldb
// * 使用 mmap 缓存 fd
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
	head Segment
	seg  segmentList
}

func OpenTSDB() *TSDB {
	return nil
}
